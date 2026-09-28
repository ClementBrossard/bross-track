"""Point d'entrée : python -m galoptrack <commande>

  daily                 collecte J-3..J-1, puis dashboard du jour (run quotidien)
  collect --from --to   collecte / rattrapage d'une période
  dashboard             régénère le dashboard (cotes du jour à jour)
  train                 réentraîne le modèle (run mensuel)
  migrate               import unique de l'historique Drive (raw/)
  status                état des tables et du modèle
  health                contrôle de santé (page /sante)
  inspect --date --hippo [--course]   montre les lignes brutes d'une course
"""

import argparse
import json
import logging
import sys
from datetime import timedelta

from . import collect, config, dashboard, health, migrate, model_store, tables, train
from .storage import get_storage


def _dates(args):
    start = config.parse_date(args.date_from)
    end = config.parse_date(args.date_to or args.date_from)
    return list(config.date_range(start, end))


def cmd_collect(storage, args):
    days = _dates(args)
    # Écriture par blocs de 7 jours : un long rattrapage interrompu garde
    # ce qui a déjà été collecté.
    for i in range(0, len(days), 7):
        summary = collect.collect_days(storage, days[i:i + 7], what=args.what.split(','))
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
