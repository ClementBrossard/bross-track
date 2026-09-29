"""Contrôle de santé du pipeline : fraîcheur des tables, bilan des derniers
jours (courses, PDF de tracking, arrivées, rapports, cotes), cohérence
tracking <-> tronçons, état du modèle. Produit :

  health/latest.json     le détail (lu par la page /sante de l'app)
  dashboards/sante.html  la page lisible
et un badge injecté dans le dashboard."""

import html as _html
import json
import os
from datetime import datetime, timedelta

import pandas as pd

from . import config, model_store, tables

OK, INFO, WARN, ERROR = 'ok', 'info', 'warn', 'error'
_LEVEL_ORDER = {OK: 0, INFO: 1, WARN: 2, ERROR: 3}

# Au-delà de ce nombre de jours sans nouvelle donnée : alerte / erreur
STALE_WARN_DAYS = 3
STALE_ERROR_DAYS = 7
RECENT_DAYS = 14
MODEL_MAX_AGE_DAYS = 45


def _d(i):
    return datetime.strptime(str(int(i)), '%Y%m%d').date()


def _is_obstacle(df):
    if 'discipline' not in df.columns:
        return pd.Series(False, index=df.index)
    disc = df['discipline'].fillna('').astype(str).str.upper()
    return (disc != '') & (disc != 'PLAT')


def _race_set(df, since=None):
    if df is None or df.empty:
        return set()
    d = pd.to_numeric(df['date'], errors='coerce')
    if since is not None:
        df = df[d >= since]
    # clé sans n° de réunion : le tracking historique ne l'a pas toujours
    return set(zip(pd.to_numeric(df['date'], errors='coerce'), df['code_hippo'].astype(str),
                   pd.to_numeric(df['num_course'], errors='coerce')))


def compute(storage, today=None, dashboard_stats=None):
    today = today or config.today_paris()
    yesterday = today - timedelta(days=1)
    alerts = []

    def alert(level, msg):
        alerts.append({'level': level, 'msg': msg})

    t = {name: tables.read_table(storage, name) for name in tables.SCHEMAS}

    # 1. Fraîcheur des tables
    fresh = {}
    for name, df in t.items():
        if df.empty:
            fresh[name] = {'rows': 0, 'date_max': None, 'age_days': None}
            alert(ERROR, f"Table « {name} » vide")
            continue
        dmax = int(pd.to_numeric(df['date'], errors='coerce').max())
        age = (yesterday - _d(dmax)).days
        fresh[name] = {'rows': int(len(df)), 'date_max': dmax, 'age_days': age}
        if age >= STALE_ERROR_DAYS:
            alert(ERROR, f"« {name} » n'est plus rafraîchie : dernière donnée le {_d(dmax):%d/%m/%Y} ({age} j de retard)")
        elif age >= STALE_WARN_DAYS:
            alert(WARN, f"« {name} » en retard : dernière donnée le {_d(dmax):%d/%m/%Y} ({age} j)")

    # 2. Cohérence tracking <-> tronçons
    since30 = int((today - timedelta(days=30)).strftime('%Y%m%d'))
    tr_all, tc_all = _race_set(t['tracking']), _race_set(t['troncons'])
    tr30, tc30 = _race_set(t['tracking'], since30), _race_set(t['troncons'], since30)
    coherence = {
        'troncons_sans_tracking': len(tc_all - tr_all),
        'tracking_sans_troncons': len(tr_all - tc_all),
        'troncons_sans_tracking_30j': len(tc30 - tr30),
        'tracking_sans_troncons_30j': len(tr30 - tc30),
    }
    if coherence['troncons_sans_tracking_30j']:
        alert(WARN, f"{coherence['troncons_sans_tracking_30j']} course(s) des 30 derniers jours ont des tronçons "
                    f"mais pas de résumé tracking : leurs tronçons sont ignorés par le modèle")
    if coherence['tracking_sans_troncons_30j']:
        alert(WARN, f"{coherence['tracking_sans_troncons_30j']} course(s) des 30 derniers jours ont un résumé "
                    f"tracking sans tronçons")
    if coherence['troncons_sans_tracking'] - coherence['troncons_sans_tracking_30j'] > 0:
        alert(INFO, f"{coherence['troncons_sans_tracking'] - coherence['troncons_sans_tracking_30j']} course(s) "
                    f"plus anciennes ont des tronçons sans résumé tracking (rattrapage possible)")

    # 3. Bilan des derniers jours
    ch = t['chevaux']
    recent = []
    for i in range(1, RECENT_DAYS + 1):
        d = today - timedelta(days=i)
        di = int(d.strftime('%Y%m%d'))
        chd = ch[pd.to_numeric(ch['date'], errors='coerce') == di] if len(ch) else ch
        races = _race_set(chd)
        if len(chd):
            arr = chd['ordre_arrivee'].notna() & (chd['ordre_arrivee'].astype(str).str.strip() != '')
            races_arr = _race_set(chd[arr])
            cote_pct = round(float(pd.to_numeric(chd.get('cote_directe'), errors='coerce').notna().mean()), 3) \
                if 'cote_directe' in chd.columns else None
        else:
            races_arr, cote_pct = set(), None
        day_set = lambda name: _race_set(t[name][pd.to_numeric(t[name]['date'], errors='coerce') == di]) \
            if len(t[name]) else set()
        row = {
            'date': di,
            'courses': len(races),
            'obstacles': len(_race_set(chd[_is_obstacle(chd)])) if len(chd) else 0,
            'avec_arrivee': len(races_arr),
            'tracking': len(day_set('tracking')),
            'troncons': len(day_set('troncons')),
            'rapports': len(day_set('rapports') & races) if races else len(day_set('rapports')),
            'cotes_pct': cote_pct,
        }
        recent.append(row)
        if i <= 7 and row['courses']:
            label = f"{d:%d/%m}"
            if row['avec_arrivee'] < row['courses']:
                alert(WARN, f"{label} : {row['courses'] - row['avec_arrivee']} course(s) sans arrivée")
            if row['tracking'] == 0:
                alert(WARN, f"{label} : aucun PDF de tracking récupéré ({row['courses']} courses)")
            if row['rapports'] < row['avec_arrivee']:
                alert(INFO if i == 1 else WARN,
                      f"{label} : rapports définitifs manquants pour {row['avec_arrivee'] - row['rapports']} course(s)")
            if cote_pct is not None and cote_pct < 0.8:
                alert(WARN, f"{label} : seulement {cote_pct:.0%} des chevaux ont une cote")

    # 4. Dernière collecte (erreurs de parsing = PDF dont le format a pu changer)
    last_collect = None
    logs = [k for k in storage.list('logs/collect_') if k.endswith('.json')]
    if logs:
        last_key = max(logs, key=lambda k: k.split('_')[-1])
        last_collect = json.loads(storage.read_bytes(last_key))
        parse_vide = sum((day.get('erreurs') or {}).get('parse_vide', 0)
                         for day in last_collect.get('days', {}).values())
        if parse_vide:
            alert(ERROR, f"{parse_vide} PDF de tracking téléchargé(s) mais illisible(s) lors de la dernière "
                         f"collecte : le format France Galop a peut-être changé")
    else:
        alert(INFO, "Aucune collecte automatique encore enregistrée")

    # 5. Modèle
    version = model_store.current_version(storage)
    model = {'version': version}
    if not version:
        alert(ERROR, "Aucun modèle en production : l'onglet Modèle est vide")
    else:
        meta_key = f"model/{version}/meta.json"
        meta = json.loads(storage.read_bytes(meta_key)) if storage.exists(meta_key) else {}
        stamp = meta.get('trained_at') or meta.get('migrated_at')
        model.update({'trained_at': stamp,
                      'hit_rate_top1': (meta.get('metrics') or {}).get('hit_rate_top1'),
                      'migrated_from_colab': meta.get('migrated_from_colab', False)})
        if stamp:
            age = (today - datetime.fromisoformat(stamp).date()).days
            model['age_days'] = age
            if age > MODEL_MAX_AGE_DAYS:
                alert(WARN, f"Modèle {version} entraîné il y a {age} jours (réentraînement mensuel en échec ?)")

    # 5b. La collecte automatique du jour a-t-elle tourné ?
    marker = 'logs/daily_last.json'
    daily_last = json.loads(storage.read_bytes(marker)) if storage.exists(marker) else None
    today_i = int(today.strftime('%Y%m%d'))
    if daily_last is None:
        alert(INFO, "Aucune collecte quotidienne automatique enregistrée pour l'instant")
    elif daily_last.get('day') != today_i:
        alert(ERROR, f"La collecte du jour n'a pas tourné : dernière collecte le "
                     f"{_d(daily_last['day']):%d/%m/%Y}. Relancer « Pipeline — quotidien » (mode daily)")

    # 6. Courses du jour
    if dashboard_stats is not None:
        if dashboard_stats.get('races_today_plat', dashboard_stats.get('races_today', 0)) and not dashboard_stats.get('horses_scored') and version:
            alert(ERROR, "Courses du jour présentes mais aucun cheval scoré par le modèle")

    worst = max((a['level'] for a in alerts), key=lambda l: _LEVEL_ORDER[l], default=OK)
    status = worst if worst in (WARN, ERROR) else OK
    alerts.sort(key=lambda a: -_LEVEL_ORDER[a['level']])
    return {
        'generated_at': config.now_paris().isoformat(timespec='seconds'),
        'today': int(today.strftime('%Y%m%d')),
        'status': status,
        'n_alerts': sum(1 for a in alerts if a['level'] in (WARN, ERROR)),
        'alerts': alerts,
        'tables': fresh,
        'coherence': coherence,
        'recent_days': recent,
        'last_collect': {'run_at': last_collect.get('run_at'), 'days': last_collect.get('days'),
                         'written': last_collect.get('written')} if last_collect else None,
        'model': model,
        'daily_last': daily_last,
        'dashboard': dashboard_stats,
    }


# ── Rendu ────────────────────────────────────────────────────────────────────

_ICON = {OK: '✅', INFO: 'ℹ️', WARN: '⚠️', ERROR: '❌'}


def badge_html(h):
    if h['status'] == OK:
        label, bg = '✅ Données OK', '#2f6e4a'
    elif h['status'] == WARN:
        label, bg = f"⚠️ {h['n_alerts']} alerte(s)", '#b8841f'
    else:
        label, bg = f"❌ {h['n_alerts']} alerte(s)", '#a8402c'
    day = h['today']
    # Si le dashboard n'a pas été régénéré aujourd'hui (robot en retard ou
    # en panne), le badge le signale directement dans le navigateur.
    script = ("<script>(function(){var d=new Date(),t=d.getFullYear()*10000+(d.getMonth()+1)*100+d.getDate();"
              f"if(t>{day}){{var b=document.getElementById('gt-health-badge');"
              "b.style.background='#a8402c';b.textContent='❌ Données d\u2019un jour précédent';}})();</script>")
    return (f'<a id="gt-health-badge" href="/sante" title="Santé des données" style="position:fixed;right:16px;'
            f'bottom:16px;z-index:9999;background:{bg};color:#fff;font:600 13px Inter,system-ui,sans-serif;'
            f'padding:8px 14px;border-radius:999px;text-decoration:none;'
            f'box-shadow:0 2px 8px rgba(0,0,0,.25)">{label}</a>{script}')


def inject_badge(dashboard_html, h):
    return dashboard_html.replace('</body>', badge_html(h) + '\n</body>', 1)


def _fmt_date(i):
    return _d(i).strftime('%d/%m/%Y') if i else '—'


def render_page(h):
    e = _html.escape
    alerts = ''.join(
        f'<li class="{a["level"]}">{_ICON[a["level"]]} {e(a["msg"])}</li>' for a in h['alerts']
    ) or '<li class="ok">✅ Aucune alerte</li>'

    table_rows = ''.join(
        f'<tr><td>{e(name)}</td><td class="n">{v["rows"]:,}</td><td>{_fmt_date(v["date_max"])}</td>'
        f'<td class="n {"bad" if (v["age_days"] or 0) >= STALE_WARN_DAYS else ""}">'
        f'{"—" if v["age_days"] is None else str(v["age_days"]) + " j"}</td></tr>'
        for name, v in h['tables'].items()
    ).replace(',', ' ')

    def cell(v, ref=None):
        bad = ref is not None and v < ref
        return f'<td class="n {"bad" if bad else ""}">{v}</td>'

    jours = ['lun', 'mar', 'mer', 'jeu', 'ven', 'sam', 'dim']
    day_rows = ''
    for r in h['recent_days']:
        dd = _d(r['date'])
        cotes = '—' if r['cotes_pct'] is None else f"{r['cotes_pct']:.0%}"
        obst = f' ({r["obstacles"]})' if r.get('obstacles') else ''
        day_rows += (f'<tr><td>{jours[dd.weekday()]} {dd:%d/%m}</td><td class="n">{r["courses"]}{obst}</td>'
                     f'{cell(r["avec_arrivee"], r["courses"])}{cell(r["tracking"])}'
                     f'{cell(r["troncons"], r["tracking"])}{cell(r["rapports"], r["avec_arrivee"])}'
                     f'<td class="n">{cotes}</td></tr>')

    c = h['coherence']
    m = h['model']
    hit = f"{m['hit_rate_top1']:.1%}" if m.get('hit_rate_top1') else '—'
    lc = h.get('last_collect') or {}
    dash = h.get('dashboard') or {}
    status_label = {OK: '✅ Tout va bien', WARN: '⚠️ À surveiller', ERROR: '❌ Problème'}[h['status']]

    return f"""<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GalopTrack — Santé des données</title>
<style>
:root{{--bg:#f6f3ea;--paper:#fffdf8;--ink:#221f1a;--soft:#6b6357;--line:#ddd6c4;--green:#1f3d2e;
--good:#2f6e4a;--gold:#b8841f;--red:#a8402c}}
@media (prefers-color-scheme: dark){{:root{{--bg:#16140f;--paper:#211e18;--ink:#eee8da;--soft:#a79e8e;
--line:#3a352b;--green:#9fc9ad;--good:#7fc79a;--gold:#e0b25a;--red:#f08c78}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);
font:14px/1.45 Inter,system-ui,sans-serif}}
.wrap{{max-width:980px;margin:0 auto;padding:24px 16px 60px}}
h1{{font-size:24px;margin:0 0 4px;color:var(--green)}}.sub{{color:var(--soft);margin:0 0 20px}}
.status{{display:inline-block;font-weight:600;padding:6px 12px;border-radius:999px;background:var(--paper);
border:1px solid var(--line);margin-bottom:16px}}
section{{background:var(--paper);border:1px solid var(--line);border-radius:8px;padding:16px;margin:0 0 16px;
overflow-x:auto}}
h2{{font-size:16px;margin:0 0 10px}}ul{{margin:0;padding-left:0;list-style:none}}li{{padding:4px 0}}
li.error{{color:var(--red);font-weight:600}}li.warn{{color:var(--gold)}}li.info{{color:var(--soft)}}
table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}}
th,td{{padding:6px 8px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}}
th{{color:var(--soft);font-weight:500;font-size:12px}}td.n,th.n{{text-align:right}}td.bad{{color:var(--red);font-weight:600}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px}}
.kpi{{border:1px solid var(--line);border-radius:6px;padding:10px 12px}}
.kpi .l{{font-size:12px;color:var(--soft)}}.kpi .v{{font-size:18px;font-weight:600}}
a{{color:var(--green)}}
</style></head><body><div class="wrap">
<h1>Santé des données</h1>
<p class="sub">Contrôle du {e(h['generated_at'][:16].replace('T', ' à '))} · <a href="/dashboard">← retour au dashboard</a></p>
<div class="status">{status_label}</div>

<section><h2>Alertes</h2><ul>{alerts}</ul></section>

<section><h2>Modèle & dashboard</h2><div class="grid">
<div class="kpi"><div class="l">Modèle en production</div><div class="v">{e(str(m.get('version') or '—'))}</div></div>
<div class="kpi"><div class="l">Réussite top-1 (validation)</div><div class="v">{hit}</div></div>
<div class="kpi"><div class="l">Âge du modèle</div><div class="v">{m.get('age_days', '—')} j</div></div>
<div class="kpi"><div class="l">Courses du jour / chevaux scorés</div>
<div class="v">{dash.get('races_today', '—')} / {dash.get('horses_scored', '—')}</div></div>
</div></section>

<section><h2>Fraîcheur des tables</h2><table>
<tr><th>Table</th><th class="n">Lignes</th><th>Dernière donnée</th><th class="n">Retard</th></tr>
{table_rows}</table></section>

<section><h2>{RECENT_DAYS} derniers jours</h2><table>
<tr><th>Jour</th><th class="n">Courses (dont obst.)</th><th class="n">Avec arrivée</th><th class="n">PDF tracking</th>
<th class="n">Tronçons</th><th class="n">Rapports</th><th class="n">Cotes</th></tr>
{day_rows}</table></section>

<section><h2>Cohérence tracking ↔ tronçons</h2><table>
<tr><th></th><th class="n">30 derniers jours</th><th class="n">Tout l'historique</th></tr>
<tr><td>Courses avec tronçons mais sans résumé tracking</td><td class="n {'bad' if c['troncons_sans_tracking_30j'] else ''}">{c['troncons_sans_tracking_30j']}</td>
<td class="n">{c['troncons_sans_tracking']}</td></tr>
<tr><td>Courses avec résumé tracking mais sans tronçons</td><td class="n">{c['tracking_sans_troncons_30j']}</td>
<td class="n">{c['tracking_sans_troncons']}</td></tr>
</table></section>

<section><h2>Dernière collecte automatique</h2>
<p>{e(str(lc.get('run_at') or 'aucune'))}</p>
<pre style="white-space:pre-wrap;font-size:12px;margin:0">{e(json.dumps(lc.get('days') or {}, ensure_ascii=False, indent=1))}</pre>
</section>
</div></body></html>"""


def publish(storage, h):
    storage.write_bytes('health/latest.json', json.dumps(h, ensure_ascii=False, indent=1).encode('utf-8'),
                        content_type='application/json')
    storage.write_bytes('dashboards/sante.html', render_page(h).encode('utf-8'),
                        content_type='text/html; charset=utf-8')


def github_annotations(h):
    """Affiche les alertes sur la page du run GitHub Actions."""
    if not os.environ.get('GITHUB_ACTIONS'):
        return
    for a in h['alerts']:
        kind = {ERROR: 'error', WARN: 'warning'}.get(a['level'])
        if kind:
            print(f"::{kind} title=GalopTrack::{a['msg']}")
