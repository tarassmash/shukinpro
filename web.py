"""Страница состояния бота: статус, цели сканера, монеты в работе, сделки, лог, кнопки Старт/Стоп."""

import collections
import html
import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

CONTROL = {"paused": os.getenv("START_PAUSED", "") == "1"}
CONTROL_KEY = os.getenv("CONTROL_KEY", "")
STATUS = {"state": "запуск", "error": "", "started": time.time()}
LOGS = collections.deque(maxlen=400)
SNAPSHOT = None
FILTERS = {}
SET_FILTER = None
_lock = threading.Lock()


def update(**kw):
    with _lock:
        STATUS.update(kw)


class Ring(logging.Handler):
    def emit(self, r):
        LOGS.appendleft(self.format(r))


def e(x):
    return html.escape(str(x))


def num(v, nd=4):
    if v is None:
        return "—"
    a = abs(v)
    return f"{v:.2f}" if a >= 1000 else f"{v:.{nd}f}" if a >= 1 else f"{v:.6f}" if a >= 0.01 else f"{v:.8f}"


def render():
    snap = SNAPSHOT() if SNAPSHOT else {"cfg": {}, "s": {}, "watch": {}}
    c, s, w = snap["cfg"], snap["s"], snap["watch"]
    with _lock:
        st = dict(STATUS)
    paused = CONTROL["paused"]
    up = int((time.time() - st["started"]) / 60)
    rows = ""
    for sym, x in sorted(w.items()):
        p = x.get("pos") or {}
        sig = x.get("sig") or {}
        upnl = p.get("upnl") if p.get("size") else None
        rows += (f"<tr><td><b>{e(sym)}</b>{' ⭐' if sym in s.get('active', []) else ''}</td>"
                 f"<td>{num(x.get('mid'))}</td><td>{e(p.get('side') or 'FLAT')} {num(p.get('size')) if p.get('size') else ''}</td>"
                 f"<td>{num(p.get('avg')) if p.get('size') else '—'}</td>"
                 f"<td class=\"{'pos' if (upnl or 0) > 0 else 'neg' if (upnl or 0) < 0 else ''}\">"
                 f"{'—' if upnl is None else f'{upnl:+.3f}'}</td>"
                 f"<td>{x.get('our_orders', 0)}/{x.get('orders', 0)}</td>"
                 f"<td>{'—' if x.get('atr') is None else f'{x['atr'] * 100:.3f}%'}</td>"
                 f"<td>{'—' if x.get('rsi') is None else f'{x['rsi']:.0f}'}</td>"
                 f"<td>{e(sig.get('oi_trend', '—'))} / {e(sig.get('cvd_trend', '—'))}</td>"
                 f"<td>{e(x.get('status', ''))}</td></tr>")
    tg = "".join(f"<tr><td>{e(t['symbol'])}</td><td>{t.get('chg', 0):+.2f}%</td>"
                 f"<td>{'—' if t.get('rsi') is None else f'{t['rsi']:.0f}'}</td></tr>" for t in s.get("targets", [])[:12])
    hist = "".join(f"<tr><td>{time.strftime('%d.%m %H:%M', time.gmtime(h['t']))}</td><td>{e(h['symbol'])}</td>"
                   f"<td>{e(h['reason'])}</td><td class=\"{'pos' if h['pnl'] >= 0 else 'neg'}\">{h['pnl']:+.3f}</td></tr>"
                   for h in reversed(s.get("history", [])[-50:]))
    key_in = '<input name="key" type="hidden" class="kk">' if CONTROL_KEY else ""
    key_box = ('<input id="k" type="password" placeholder="пароль CONTROL_KEY" autocomplete="current-password">'
               if CONTROL_KEY else "")
    btn = "start" if paused else "stop"
    err = f'<div class="err">{e(st["error"])}</div>' if st.get("error") else ""
    fb = ""
    for k, label in FILTERS.items():
        on = bool(c.get(k))
        fb += (f'<form method="post" action="/filter"><input type="hidden" name="name" value="{k}">{key_in}'
               f'<button class="f {"on" if on else "off"}">{e(label)}: {"ВКЛ" if on else "ВЫКЛ"}</button></form>')
    all_on = all(c.get(k) for k in FILTERS)
    fb += (f'<form method="post" action="/filter"><input type="hidden" name="name" value="all">{key_in}'
           f'<button class="f all">{"Выключить все фильтры" if all_on else "Включить все фильтры"}</button></form>')
    if not c.get("shukin"):
        fb += '<div class="note">Щукин-фильтр работает только в режиме Щукина (SHUKIN=1)</div>' 
    mode = ("ЩУКИН" if c.get("shukin") else "обычный") + " · " + ("LIVE" if c.get("live") else "SIM — ордеров нет (LIVE=0)")
    return f"""<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="10"><title>Sell Grid — Short 24/7</title><style>
body{{margin:0;background:#0b0e11;color:#eaecef;font:14px -apple-system,Segoe UI,Roboto,sans-serif}}
.wrap{{max-width:1100px;margin:auto;padding:16px}}h1{{font-size:18px}}h1 span{{color:#f6465d}}
.p{{background:#12161c;border:1px solid #1e2329;border-radius:10px;padding:12px;margin:10px 0;overflow:auto}}
h2{{font-size:12px;text-transform:uppercase;color:#848e9c;margin:0 0 8px}}
.kv{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px}}.kv span{{color:#848e9c;font-size:12px;display:block}}
table{{width:100%;border-collapse:collapse;font-size:13px}}td,th{{padding:5px 6px;border-bottom:1px solid #1e2329;text-align:left;white-space:nowrap}}
th{{color:#848e9c;font-weight:500}}.pos{{color:#0ecb81}}.neg{{color:#f6465d}}
.err{{background:rgba(246,70,93,.15);color:#f6465d;padding:10px;border-radius:8px}}
button{{background:{'#0ecb81' if paused else '#2b3139'};color:{'#000' if paused else '#eaecef'};border:0;border-radius:8px;padding:9px 18px;font-weight:600;cursor:pointer}}
input{{background:#1a1f27;border:1px solid #1e2329;color:#eaecef;border-radius:6px;padding:8px;margin-right:6px}}
.fl{{display:flex;flex-wrap:wrap;gap:8px;align-items:center}}.fl form{{display:flex;gap:6px}}
button.f{{border-radius:8px;padding:9px 14px;font-weight:600;border:1px solid #1e2329}}
button.f.on{{background:rgba(14,203,129,.15);color:#0ecb81;border-color:#0ecb81}}
button.f.off{{background:rgba(246,70,93,.12);color:#f6465d;border-color:#f6465d}}
button.f.all{{background:#1a1f27;color:#eaecef}}.note{{color:#848e9c;font-size:12px}}
pre{{margin:0;max-height:380px;overflow:auto;font:12px/1.5 ui-monospace,monospace;color:#c9d6d0;white-space:pre-wrap}}
</style></head><body><div class="wrap">
<h1>Sell <span>Grid</span> — Short 24/7</h1>{err}
<div class="p fl">{key_box}<form method="post" action="/{btn}">{key_in}<button>{'▶ Старт' if paused else '■ Стоп'}</button></form></div>
<div class="p"><h2>Фильтры входа (клик — вкл/выкл)</h2><div class="fl">{fb}</div></div>
<div class="p kv">
<div><span>Состояние</span><b>{e(st['state'])}</b></div><div><span>Режим</span><b>{e(mode)}</b></div>
<div><span>Сетка</span><b>шаг {c.get('grid_step_pct')}% × {c.get('grid_levels')} · тейк {c.get('tp_pct')}% · стоп {c.get('sl_pct')}%</b></div>
<div><span>На уровень / лимит дня</span><b>{c.get('order_usdt')}$ / {c.get('max_daily_loss')}$</b></div>
<div><span>День PnL</span><b class="{'pos' if s.get('day_pnl', 0) >= 0 else 'neg'}">{s.get('day_pnl', 0):+.2f}$</b></div>
<div><span>Сделок · Win/Loss</span><b>{s.get('trades', 0)} · {s.get('wins', 0)}/{s.get('losses', 0)}</b></div>
<div><span>Циклов · работает</span><b>{s.get('cycles', 0)} · {up} мин</b></div>
<div><span>Сканер</span><b>{e(c.get('symbol') or c.get('scan_mode'))}</b></div></div>
<div class="p"><h2>Монеты в работе (⭐ — от сканера)</h2><table><tr><th>Монета</th><th>Mid</th><th>Позиция</th><th>Вход</th><th>uPnL</th>
<th>Мои/все орд.</th><th>ATR</th><th>RSI</th><th>OI / CVD</th><th>Статус</th></tr>{rows or '<tr><td colspan=10>нет</td></tr>'}</table></div>
<div class="p"><h2>Цели сканера</h2><table><tr><th>Монета</th><th>Изм.</th><th>RSI</th></tr>{tg or '<tr><td colspan=3>—</td></tr>'}</table></div>
<div class="p"><h2>Сделки</h2><table><tr><th>Время UTC</th><th>Монета</th><th>Выход</th><th>PnL $</th></tr>{hist or '<tr><td colspan=4>пока нет</td></tr>'}</table></div>
<div class="p"><h2>Лог</h2><pre>{e(chr(10).join(LOGS))}</pre></div></div>
<script>(function(){{var k=document.getElementById('k');if(!k)return;
try{{k.value=localStorage.getItem('sg_ck')||''}}catch(_){{}}
document.querySelectorAll('form').forEach(function(f){{f.addEventListener('submit',function(){{
try{{localStorage.setItem('sg_ck',k.value)}}catch(_){{}}
f.querySelectorAll('.kk').forEach(function(h){{h.value=k.value}})}})}})}})();</script></body></html>"""


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="text/html; charset=utf-8"):
        b = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            return self._send(200, "ok", "text/plain")
        if path == "/api":
            snap = SNAPSHOT() if SNAPSHOT else {}
            return self._send(200, json.dumps({"status": STATUS, "paused": CONTROL["paused"], **snap}, default=str),
                              "application/json")
        try:
            self._send(200, render())
        except Exception as ex:  # noqa: BLE001
            self._send(500, f"error: {e(ex)}")

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        form = parse_qs(self.rfile.read(n).decode()) if n else {}
        if CONTROL_KEY and (form.get("key") or [""])[0] != CONTROL_KEY:
            return self._send(403, "Неверный CONTROL_KEY. <a href='/'>Назад</a>")
        path = urlparse(self.path).path
        if path == "/filter" and SET_FILTER:
            SET_FILTER((form.get("name") or [""])[0])
        if path in ("/start", "/stop"):
            CONTROL["paused"] = path == "/stop"
            logging.getLogger("sg").info("%s со страницы", "Старт" if path == "/start" else "Стоп")
        self.send_response(303)
        self.send_header("Location", "/")
        self.end_headers()


def start():
    h = Ring()
    h.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%H:%M:%S"))
    logging.getLogger().addHandler(h)
    port = int(os.getenv("PORT") or 8080)
    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    logging.getLogger("sg").info("Страница состояния на порту %d", port)
