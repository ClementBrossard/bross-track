"""Point d'entrée : python -m galoptrack <commande>

  daily                 collecte J-3..J-1, puis dashboard du jour (run quotidien)
  collect --from --to   collecte / rattrapage d'une période
  dashboard             régénère le dashboard (cotes du jour à jour)
  train                 réentraîne le modèle (run mensuel)
  promote --version vN  met une version déjà entraînée en production (ou revient en arrière)
  migrate               import unique de l'historique Drive (raw/)
  status                état des tables et du modèle
  backup [--label | --list]           sauvegarde des tables (backups/<nom>/)
  restore --name                      remet les tables d'une sauvegarde
  health                contrôle de santé (page /sante)
  inspect --date --hippo [--course]   montre les lignes brutes d'une course
"""

import argparse
import json
import logging
import sys
from datetime import timedelta

from . import audit, collect, config, experiment, dashboard, health, migrate, model_store, tables, train
from .storage import get_storage


def _dates(args):
    start = config.parse_date(args.date_from)
    end = config.parse_date(args.date_to or args.date_from)
    return list(config.date_range(start, end))


def _disciplines(v):
    """'plat', 'obstacle', 'plat,obstacle' ou 'tout' -> ensemble (None = tout)."""
    if not v or v.strip().lower() in ('tout', 'all'):
        return None
    out = {x.strip().lower() for x in v.split(',') if x.strip()}
    bad = out - {'plat', 'obstacle'}
    if bad:
        raise SystemExit(f"Discipline inconnue : {', '.join(sorted(bad))} (plat, obstacle ou tout)")
    return out


def cmd_collect(storage, args):
    days = _dates(args)
    # Écriture par blocs de 7 jours : un long rattrapage interrompu garde
    # ce qui a déjà été collecté.
    for i in range(0, len(days), 7):
        summary = collect.collect_days(storage, days[i:i + 7], what=args.what.split(','),
                                       disciplines=_disciplines(args.disciplines))
        print(json.dumps({k: v for k, v in summary.items() if k != 'errors'}, indent=1))
        print(f"{len(summary['errors'])} erreur(s)/absence(s) — détail dans logs/ du stockage")
        for e in summary['errors'][:30]:
            print("  ", e)


def _publish_with_health(storage, html, stats, day):
    h = health.compute(storage, today=day, dashboard_stats=stats)
    health.publish(storage, h)
    dashboard.publish(storage, health.inject_badge(html, h), stats)
    health.github_annotations(h)
    print(f"Santé : {h['status']} ({h['n_alerts']} alerte(s))")
    for a in h['alerts']:
        print(f"  [{a['level']}] {a['msg']}")


def cmd_health(storage, args):
    h = health.compute(storage)
    health.publish(storage, h)
    health.github_annotations(h)
    print(json.dumps({k: h[k] for k in ('status', 'n_alerts', 'alerts', 'tables', 'coherence')},
                     ensure_ascii=False, indent=1))


def cmd_inspect(storage, args):
    """Diagnostic : lignes brutes de chaque table pour une course, puis les
    courses que le dashboard en construit."""
    import pandas as pd
    from .races import build_races
    d = int(config.yyyymmdd(config.parse_date(args.date)))
    pd.set_option('display.width', 250)
    pd.set_option('display.max_columns', 30)
    pd.set_option('display.max_rows', 200)
    show = {
        'tracking': ['date', 'code_hippo', 'num_reunion', 'num_course', 'nom_cheval', 'position_arrivee',
                     'vitesse_moyenne_kmh', 'nb_troncons'],
        'troncons': ['date', 'code_hippo', 'num_reunion', 'num_course', 'nom_cheval', 'troncon_index',
                     'troncon_label', 'vitesse_kmh'],
        'chevaux': ['date', 'code_hippo', 'num_reunion', 'num_course', 'nom_cheval', 'ordre_arrivee',
                    'numero_partant', 'cote_directe'],
        'rapports': ['date', 'code_hippo', 'num_reunion', 'num_course', 'nom_cheval', 'num_pmu'],
    }
    sub = {}
    for name, cols in show.items():
        df = tables.read_table(storage, name)
        m = (pd.to_numeric(df['date'], errors='coerce') == d) & (df['code_hippo'].astype(str) == args.hippo)
        if args.course:
            m &= pd.to_numeric(df['num_course'], errors='coerce') == args.course
        sub[name] = df[m]
        print(f"\n===== {name} : {int(m.sum())} ligne(s) =====")
        if name == 'troncons':
            print(df[m].groupby(['num_reunion', 'num_course', 'nom_cheval']).size().to_string())
        else:
            print(df.loc[m, cols].to_string())
    races_list, _, _ = build_races(sub['tracking'], sub['troncons'], sub['chevaux'])
    print("\n===== courses construites pour le dashboard =====")
    for r in races_list:
        print(r['id'], '| trackée' if r.get('tracked') else '| sans tracking', '|', len(r['horses']), 'chevaux |',
              [(h['nom'], h['pa'], len(h['tr'])) for h in r['horses']])


def cmd_dashboard(storage, args):
    day = config.parse_date(args.day)
    html, stats = dashboard.build(storage, day=day, with_today=not args.no_today)
    _publish_with_health(storage, html, stats, day)
    print(json.dumps(stats, indent=1))


DAILY_MARKER = 'logs/daily_last.json'


def cmd_daily(storage, args):
    today = config.today_paris()
    today_i = int(config.yyyymmdd(today))
    if args.if_needed and storage.exists(DAILY_MARKER):
        last = json.loads(storage.read_bytes(DAILY_MARKER))
        if last.get('day') == today_i:
            print(f"Collecte du jour déjà faite ({last.get('finished_at')}) : rien à faire.")
            return
    days = [today - timedelta(days=i) for i in range(config.LOOKBACK_DAYS, 0, -1)]
    summary = collect.collect_days(storage, days)
    print("Collecte :", json.dumps(summary['days']), "écrit :", json.dumps(summary['written']))
    storage.write_bytes(DAILY_MARKER, json.dumps({
        'day': today_i, 'finished_at': config.now_paris().isoformat(timespec='seconds')}).encode('utf-8'),
        content_type='application/json')
    html, stats = dashboard.build(storage, day=today)
    _publish_with_health(storage, html, stats, today)
    print("Dashboard :", json.dumps(stats))


BACKUP_PREFIX = 'backups/'
BACKUP_KEYS = [tables.table_key(n) for n in tables.SCHEMAS] + ['model/current.json']


def list_backups(storage):
    return sorted({k[len(BACKUP_PREFIX):].split('/', 1)[0] for k in storage.list(BACKUP_PREFIX)})


def backup(storage, label=''):
    """Copie les tables (et le pointeur du modèle en production) dans
    backups/<date-heure>[-label]/. Retourne le nom de la sauvegarde."""
    name = config.now_paris().strftime('%Y%m%d-%H%M') + (f'-{label}' if label else '')
    for key in BACKUP_KEYS:
        if storage.exists(key):
            storage.write_bytes(f'{BACKUP_PREFIX}{name}/{key}', storage.read_bytes(key))
    return name


def restore(storage, name):
    keys = [k for k in storage.list(f'{BACKUP_PREFIX}{name}/')]
    if not keys:
        raise SystemExit(f"Sauvegarde introuvable : {name} (disponibles : {', '.join(list_backups(storage)) or 'aucune'})")
    for k in keys:
        storage.write_bytes(k[len(f'{BACKUP_PREFIX}{name}/'):], storage.read_bytes(k))
    return len(keys)


def cmd_backup(storage, args):
    if args.list:
        print('\n'.join(list_backups(storage)) or 'Aucune sauvegarde')
        return
    print("Sauvegarde :", backup(storage, args.label))


def cmd_restore(storage, args):
    n = restore(storage, args.name)
    print(f"{n} fichier(s) restauré(s) depuis {args.name}. Relancer « dashboard » pour régénérer le site.")


def cmd_audit_hippos(storage, args):
    rows = audit.audit(config.today_paris(), n_days=args.days, step=args.step)
    print(audit.report(rows))


def cmd_experiment(storage, args):
    print(experiment.run(storage))


def cmd_experiment_opp(storage, args):
    print(experiment.run_opposition(storage))


def cmd_experiment_classe(storage, args):
    print(experiment.run_classe(storage))


def cmd_experiment_confront(storage, args):
    print(experiment.run_confront(storage))


def cmd_experiment_top3(storage, args):
    print(experiment.run_top3(storage))


def cmd_experiment_troncons(storage, args):
    print(experiment.run_troncons(storage))


def cmd_experiment_perf(storage, args):
    print(experiment.run_perf(storage))


def cmd_experiment_marche(storage, args):
    print(experiment.run_marche(storage))


def cmd_experiment_sonde_cotes(storage, args):
    print(experiment.run_sonde_cotes(storage))


def cmd_experiment_seuils(storage, args):
    print(experiment.run_seuils(storage, args.version))


def cmd_experiment_rentabilite(storage, args):
    print(experiment.run_rentabilite(storage, args.version))


def cmd_promote(storage, args):
    versions = model_store.list_versions(storage)
    if args.version not in versions:
        raise SystemExit(f"Version inconnue : {args.version} (disponibles : {', '.join(versions)})")
    before = model_store.current_version(storage)
    model_store.set_current(storage, args.version)
    print(f"Modèle en production : {before} -> {args.version}")


def cmd_rename_version(storage, args):
    try:
        n = model_store.rename_version(storage, args.src, args.dst)
    except ValueError as e:
        raise SystemExit(str(e))
    print(f"{args.src} renommée en {args.dst} ({n} fichiers). En production : {model_store.current_version(storage)}")


def cmd_models(storage, args):
    cur = model_store.current_version(storage)
    for v in model_store.list_versions(storage):
        meta = model_store._j(storage, f"model/{v}/meta.json") if storage.exists(f"model/{v}/meta.json") else {}
        m = meta.get('metrics', {})
        t3 = m.get('top3_in_top3')
        print(f"{v:8s} {'EN PRODUCTION' if v == cur else '':14s} entraîné {str(meta.get('trained_at', '?'))[:10]}"
              f"  {len(meta.get('features', []))} variables  n°1 gagnant {m.get('hit_rate_top1') or 0:.1%}"
              + (f"  top 3 {t3:.3f}/3" if t3 is not None else ''))


def run_ops(storage, demande):
    """Exécute une demande d'opérations (ops/demande.json), une seule fois :
    la trace est gardée dans logs/ops/<id>.json. Étapes : renommer (de, vers),
    entrainer (production true/false), mettre_en_production (version)."""
    done_key = f"logs/ops/{demande['id']}.json"
    if storage.exists(done_key):
        print(f"Demande {demande['id']} déjà exécutée : rien à faire.")
        return False
    journal = []
    for e in demande['etapes']:
        a = e['action']
        if a == 'renommer':
            versions = model_store.list_versions(storage)
            if e['de'] not in versions and e['vers'] in versions:
                journal.append(f"renommer {e['de']} -> {e['vers']} : déjà fait")
                continue
            n = model_store.rename_version(storage, e['de'], e['vers'])
            journal.append(f"renommer {e['de']} -> {e['vers']} : {n} fichiers")
        elif a == 'entrainer':
            meta = train.run(storage, promote=bool(e.get('production')))
            journal.append(f"entrainer : {meta['version']} ({'EN PRODUCTION' if meta['promoted'] else 'pas en production'})")
            print(json.dumps({k: meta[k] for k in ('version', 'metrics', 'temoin_v8', 'promoted') if k in meta},
                             indent=1, default=str))
        elif a == 'mettre_en_production':
            if e['version'] not in model_store.list_versions(storage):
                raise SystemExit(f"Version inconnue : {e['version']}")
            model_store.set_current(storage, e['version'])
            journal.append(f"mettre_en_production {e['version']}")
        else:
            raise SystemExit(f"Action inconnue : {a}")
        print(journal[-1])
    storage.write_bytes(done_key, json.dumps({'demande': demande, 'journal': journal,
                                              'at': config.now_paris().isoformat(timespec='seconds')},
                                             ensure_ascii=False, indent=1).encode('utf-8'),
                        content_type='application/json')
    return True


def cmd_ops(storage, args):
    with open(args.fichier, encoding='utf-8') as f:
        run_ops(storage, json.load(f))


def cmd_train(storage, args):
    meta = train.run(storage, promote=not args.no_promote)
    print(json.dumps(meta, indent=1, default=str))


def cmd_migrate(storage, args):
    _, md = migrate.run(storage)
    print(md)


def cmd_status(storage, args):
    out = {}
    for name in tables.SCHEMAS:
        df = tables.read_table(storage, name)
        out[name] = migrate._describe(df) if len(df) else {'rows': 0}
        out[name].pop('hippos', None)
    out['model_current'] = model_store.current_version(storage)
    out['model_versions'] = model_store.list_versions(storage)
    print(json.dumps(out, indent=1))


def main(argv=None):
    p = argparse.ArgumentParser(prog='galoptrack')
    p.add_argument('--storage', choices=['local', 'r2'], help="défaut : r2 si R2_BUCKET défini, sinon local")
    sub = p.add_subparsers(dest='cmd', required=True)

    s = sub.add_parser('collect')
    s.add_argument('--from', dest='date_from', required=True, help="YYYY-MM-DD, 'yesterday'...")
    s.add_argument('--to', dest='date_to')
    s.add_argument('--what', default='tracking,participants,rapports')
    s.add_argument('--disciplines', default='tout', help="plat, obstacle ou tout (défaut)")
    s.set_defaults(func=cmd_collect)

    s = sub.add_parser('dashboard')
    s.add_argument('--day', default='today')
    s.add_argument('--no-today', action='store_true', help="sans les courses du jour (pas d'appel PMU)")
    s.set_defaults(func=cmd_dashboard)

    s = sub.add_parser('daily')
    s.add_argument('--if-needed', action='store_true',
                   help="ne fait rien si la collecte du jour a déjà tourné (créneaux de secours)")
    s.set_defaults(func=cmd_daily)

    s = sub.add_parser('train')
    s.add_argument('--no-promote', action='store_true', help="enregistre la version sans la mettre en production")
    s.set_defaults(func=cmd_train)

    s = sub.add_parser('backup')
    s.add_argument('--label', default='')
    s.add_argument('--list', action='store_true', help="liste les sauvegardes")
    s.set_defaults(func=cmd_backup)

    s = sub.add_parser('restore')
    s.add_argument('--name', required=True, help="nom affiché par « backup --list »")
    s.set_defaults(func=cmd_restore)

    s = sub.add_parser('audit-hippos', help="hippodromes vus chez PMU + test des PDF France Galop")
    s.add_argument('--days', type=int, default=365)
    s.add_argument('--step', type=int, default=2)
    s.set_defaults(func=cmd_audit_hippos)

    sub.add_parser('experiment-poids', help="compare le modèle avec / sans poids relatif (n'écrit rien)") \
        .set_defaults(func=cmd_experiment)
    sub.add_parser('experiment-adversaires', help="niveau des adversaires affrontés vs peloton du jour (n'écrit rien)") \
        .set_defaults(func=cmd_experiment_opp)
    sub.add_parser('experiment-classe', help="note de classe turfiste (étape 1 du modèle Compétition, n'écrit rien)") \
        .set_defaults(func=cmd_experiment_classe)
    sub.add_parser('experiment-confrontations', help="confrontations (étape 2 du modèle Compétition, n'écrit rien)") \
        .set_defaults(func=cmd_experiment_confront)
    sub.add_parser('experiment-top3', help="objectif 3 premiers : modèle appris « gagnant » vs « top 3 » (n'écrit rien)") \
        .set_defaults(func=cmd_experiment_top3)
    sub.add_parser('experiment-troncons', help="tronçons : vitesses relatives et scénario de course (n'écrit rien)") \
        .set_defaults(func=cmd_experiment_troncons)
    sub.add_parser('experiment-sonde_cotes', help="l'API PMU garde-t-elle la cote du matin des courses passées ?") \
        .set_defaults(func=cmd_experiment_sonde_cotes)
    sub.add_parser('experiment-marche', help="challenger étape 0 : la cote seule, v9 + cote (n'écrit rien)") \
        .set_defaults(func=cmd_experiment_marche)
    sub.add_parser('experiment-perf', help="chevaux battus corrigés du poids et de la valeur handicap (n'écrit rien)") \
        .set_defaults(func=cmd_experiment_perf)
    for name, fn, hlp in (('experiment-seuils', cmd_experiment_seuils, "% de placés selon la proba du modèle"),
                          ('experiment-rentabilite', cmd_experiment_rentabilite,
                           "simple placé : seuils de proba et de cote rentables")):
        s = sub.add_parser(name, help=hlp + " (n'écrit rien)")
        s.add_argument('--version', default=None, help="version du modèle (défaut : celle en production)")
        s.set_defaults(func=fn)

    s = sub.add_parser('rename-version', help="renomme une version de modèle (ex. v9 -> v8_1)")
    s.add_argument('--from', dest='src', required=True)
    s.add_argument('--to', dest='dst', required=True)
    s.set_defaults(func=cmd_rename_version)
    sub.add_parser('models', help="liste les versions de modèle").set_defaults(func=cmd_models)
    s = sub.add_parser('ops', help="exécute une demande d'opérations (ops/demande.json), une seule fois")
    s.add_argument('fichier', nargs='?', default='ops/demande.json')
    s.set_defaults(func=cmd_ops)

    s = sub.add_parser('promote', help="met une version déjà entraînée en production (ex. v9)")
    s.add_argument('--version', required=True)
    s.set_defaults(func=cmd_promote)

    sub.add_parser('migrate').set_defaults(func=cmd_migrate)
    sub.add_parser('status').set_defaults(func=cmd_status)
    sub.add_parser('health').set_defaults(func=cmd_health)

    s = sub.add_parser('inspect')
    s.add_argument('--date', required=True)
    s.add_argument('--hippo', required=True)
    s.add_argument('--course', type=int)
    s.set_defaults(func=cmd_inspect)

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-7s %(name)s  %(message)s',
                        datefmt='%H:%M:%S', stream=sys.stderr)
    storage = get_storage(args.storage)
    logging.getLogger('galoptrack').info("Stockage : %r", storage)
    args.func(storage, args)


if __name__ == '__main__':
    main()
