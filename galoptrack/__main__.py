"""Point d'entrée : python -m galoptrack <commande>

  daily                 collecte J-3..J-1, puis dashboard du jour (run quotidien)
  collect --from --to   collecte / rattrapage d'une période
  dashboard             régénère le dashboard (cotes du jour à jour)
  train                 réentraîne le modèle (run mensuel)
  migrate               import unique de l'historique Drive (raw/)
  status                état des tables et du modèle
  health                contrôle de santé (page /sante)
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


def cmd_dashboard(storage, args):
    day = config.parse_date(args.day)
    html, stats = dashboard.build(storage, day=day, with_today=not args.no_today)
    _publish_with_health(storage, html, stats, day)
    print(json.dumps(stats, indent=1))


def cmd_daily(storage, args):
    today = config.today_paris()
    days = [today - timedelta(days=i) for i in range(config.LOOKBACK_DAYS, 0, -1)]
    summary = collect.collect_days(storage, days)
    print("Collecte :", json.dumps(summary['days']), "écrit :", json.dumps(summary['written']))
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

    sub.add_parser('daily').set_defaults(func=cmd_daily)

    s = sub.add_parser('train')
    s.add_argument('--no-promote', action='store_true', help="enregistre la version sans la mettre en production")
    s.set_defaults(func=cmd_train)

    sub.add_parser('migrate').set_defaults(func=cmd_migrate)
    sub.add_parser('status').set_defaults(func=cmd_status)
    sub.add_parser('health').set_defaults(func=cmd_health)

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-7s %(name)s  %(message)s',
                        datefmt='%H:%M:%S', stream=sys.stderr)
    storage = get_storage(args.storage)
    logging.getLogger('galoptrack').info("Stockage : %r", storage)
    args.func(storage, args)


if __name__ == '__main__':
    main()
