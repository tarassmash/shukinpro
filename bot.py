"""Sell Grid — Bybit Short (режим Щукина), 24/7 на сервере.

Перенос логики страницы sell-grid-shukin.html на Python:
  - сетка SELL-лимиток выше цены (ловит откат после пампа), пересобирается вокруг текущей цены каждый цикл;
  - в шорте: тейк / стоп от средней цены входа (закрытие по рынку), мейкер-тейк лимиткой PostOnly;
  - режим Щукина: шаг от 1%, тейк от 0.7%, аварийный стоп 10%, добор выше цены при открытом шорте,
    фильтр OI + CVD (не шортить, пока OI растёт / CVD в сильных покупках);
  - защита от спайка, фильтр ATR, дневной лимит убытка;
  - сканер «пампы под шорт» или «ультра-волатильные 1m» сам выбирает монету.

Отличие от страницы: бот трогает только свои монеты (где он сам ставил ордера), а не все позиции счёта —
чтобы не мешать другим ботам на этом же аккаунте (MANAGE_ALL=1 вернёт поведение страницы).
"""

import json
import logging
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import ROUND_CEILING, ROUND_HALF_UP, Decimal

from pybit.unified_trading import HTTP

import web

log = logging.getLogger("sg")


# ---------------------------------------------------------------- настройки (переменные Railway)

def env(name, default):
    v = os.getenv(name)
    if v is None or v == "":
        return default
    if isinstance(default, bool):
        return v.strip().lower() in ("1", "true", "yes", "on", "да")
    if isinstance(default, int):
        return int(float(v))
    if isinstance(default, float):
        return float(v)
    return v.strip()


SHUKIN = env("SHUKIN", True)
# значения по умолчанию — как на странице; режим Щукина ставит свои (как кнопка «Режим Щукина»)
_D = dict(step=1.0, levels=5, tp=0.7, sl=10.0, usdt=12.0, spike=0.5, min_atr=0.2, max_atr=5.0) if SHUKIN else \
    dict(step=0.25, levels=4, tp=0.3, sl=1.0, usdt=20.0, spike=0.4, min_atr=0.15, max_atr=4.0)

CFG = {
    "testnet": env("TESTNET", False),
    "live": env("LIVE", False),                    # реальные ордера; без этого — симуляция (только лог)
    "category": env("CATEGORY", "linear"),
    "symbol": env("SYMBOL", "").upper(),           # пусто — монету выбирает сканер
    "interval": env("INTERVAL", "5"),
    "shukin": SHUKIN,
    "grid_step_pct": env("GRID_STEP_PCT", _D["step"]),
    "grid_levels": env("GRID_LEVELS", _D["levels"]),
    "tp_pct": env("TP_PCT", _D["tp"]),
    "sl_pct": env("SL_PCT", _D["sl"]),
    "order_usdt": env("ORDER_USDT", _D["usdt"]),
    "max_daily_loss": env("MAX_DAILY_LOSS", 15.0),
    "cycle_sec": max(2, env("CYCLE_SEC", 3)),
    "maker_exit": env("MAKER_EXIT", True),
    "shukin_filter": env("SHUKIN_FILTER", True),   # фильтр OI + CVD в режиме Щукина
    "spike_protect": env("SPIKE_PROTECT", True),
    "max_spike_pct": env("MAX_SPIKE_PCT", _D["spike"]),
    "vol_filter": env("VOL_FILTER", True),
    "min_atr_pct": env("MIN_ATR_PCT", _D["min_atr"]),
    "max_atr_pct": env("MAX_ATR_PCT", _D["max_atr"]),
    "scan_mode": env("SCAN_MODE", "pump"),         # pump | ultra | off
    "scan_every_min": env("SCAN_EVERY_MIN", 5.0),
    "max_active": max(1, env("MAX_ACTIVE", 10)),    # максимум монет одновременно (реально — сколько позволит депозит)
    "sizing": env("SIZING", "auto"),               # auto — размер от депозита; fixed — ORDER_USDT на уровень
    "deposit_pct": env("DEPOSIT_PCT", 90.0),       # какую часть свободного баланса задействовать, %
    "leverage": env("LEVERAGE", 3.0),              # плечо на монетах бота
    "min_level_usdt": env("MIN_LEVEL_USDT", 6.0),  # минимальный уровень сетки, $ (Bybit не берёт ордера < 5$)
    "manage_all": env("MANAGE_ALL", False),
}
DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or ("/data" if os.path.isdir("/data") else "data")
STATE_FILE = os.path.join(DATA_DIR, "sell_grid_state.json")


# ---------------------------------------------------------------- состояние

S = {
    "cycles": 0, "trades": 0, "wins": 0, "losses": 0, "day_pnl": 0.0, "day_key": "",
    "own": [],            # монеты, где бот сам ставил ордера (их и только их контролирует)
    "targets": [],        # последний скан
    "active": [],         # монеты, на которых сейчас ставим сетку
    "last_scan": 0.0,
    "history": [],        # закрытые сделки
}
WATCH = {}                # symbol -> {mid, candles, price_hist, pos, orders, our_orders, spike, vol_ok, atr, rsi, status, sig}
INST = {}
LOCK = threading.RLock()


SAVE_LOCK = threading.Lock()


def save_state():
    with SAVE_LOCK:
        _save_state()


def _save_state():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = STATE_FILE + f".{threading.get_ident()}.tmp"
        with open(tmp, "w") as f:
            json.dump({**{k: S[k] for k in ("trades", "wins", "losses", "day_pnl", "day_key", "own", "history")},
                       "filters": {k: CFG[k] for k in FILTERS}}, f)
        os.replace(tmp, STATE_FILE)
    except Exception as e:  # noqa: BLE001
        log.warning("состояние не сохранилось: %s", e)


FILTERS = {"shukin_filter": "Щукин-фильтр OI/CVD", "spike_protect": "Защита от спайка", "vol_filter": "Фильтр ATR"}


def set_filter(name, on=None):
    """Кнопки на странице: включить/выключить фильтр (name='all' — все сразу). Сохраняется на диск."""
    with LOCK:
        names = list(FILTERS) if name == "all" else [name] if name in FILTERS else []
        if not names:
            return
        if on is None:
            on = not all(CFG[n] for n in names) if name == "all" else not CFG[name]
        for n in names:
            CFG[n] = bool(on)
            for w in WATCH.values():
                w["_logged"] = None
        save_state()
    log.info("Фильтры со страницы: %s", ", ".join(f"{FILTERS[n]} {'ВКЛ' if CFG[n] else 'ВЫКЛ'}" for n in names))


def load_state():
    try:
        with open(STATE_FILE) as f:
            d = json.load(f)
        for k, v in (d.pop("filters", None) or {}).items():
            if k in FILTERS:
                CFG[k] = bool(v)
        S.update(d)
    except FileNotFoundError:
        pass
    except Exception as e:  # noqa: BLE001
        log.warning("состояние не прочитано: %s", e)


def ensure_day():
    k = time.strftime("%Y-%m-%d", time.gmtime())
    if S["day_key"] != k:
        S.update(day_key=k, day_pnl=0.0, trades=0, wins=0, losses=0)
        save_state()


def record_trade(sym, pnl, reason):
    ensure_day()
    S["trades"] += 1
    S["day_pnl"] += pnl
    S["wins" if pnl >= 0 else "losses"] += 1
    S["history"] = (S["history"] + [{"t": int(time.time()), "symbol": sym, "pnl": pnl, "reason": reason}])[-100:]
    save_state()
    log.info("%s: сделка %s PnL %+.3f", sym, reason, pnl)


# ---------------------------------------------------------------- биржа

def make_session():
    key, sec = os.getenv("BYBIT_API_KEY", ""), os.getenv("BYBIT_API_SECRET", "")
    return HTTP(testnet=CFG["testnet"], api_key=key or None, api_secret=sec or None, recv_window=10000), bool(key and sec)


SESSION, HAS_KEYS = None, False


def api(fn, **kw):
    """Вызов Bybit. Ошибку биржи (retCode != 0) отдаём как исключение с её текстом."""
    for i in range(3):
        try:
            r = fn(**kw)
            if r.get("retCode", 0) != 0:
                raise RuntimeError(r.get("retMsg") or str(r))
            return r
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if i == 2 or not any(x in msg for x in ("timed out", "Connection", "10006", "10002", "Read timed")):
                raise
            time.sleep(1 + i)


def round_step(v, step):
    """Как roundToStep на странице, но точно (Decimal)."""
    st = Decimal(str(step))
    return float((Decimal(str(v)) / st).to_integral_value(ROUND_HALF_UP) * st)


def fmt_qty(q, step):
    st = Decimal(str(step))
    return str((Decimal(str(q)) / st).to_integral_value(ROUND_HALF_UP) * st).rstrip("0").rstrip(".") or "0"


def instrument(sym):
    if sym not in INST:
        r = api(SESSION.get_instruments_info, category=CFG["category"], symbol=sym)
        info = (r["result"]["list"] or [None])[0]
        if not info:
            raise RuntimeError(f"{sym}: инструмент не найден")
        INST[sym] = {"tick": info["priceFilter"]["tickSize"], "qty_step": info["lotSizeFilter"]["qtyStep"],
                     "min_qty": info["lotSizeFilter"]["minOrderQty"]}
    return INST[sym]


def orderbook(sym):
    r = api(SESSION.get_orderbook, category=CFG["category"], symbol=sym, limit=5)
    b = [float(x[0]) for x in r["result"].get("b", [])]
    a = [float(x[0]) for x in r["result"].get("a", [])]
    return b, a


def klines(sym, interval, limit):
    r = api(SESSION.get_kline, category=CFG["category"], symbol=sym, interval=str(interval), limit=limit)
    out = {}
    for k in r["result"]["list"]:
        t = int(k[0]) // 1000
        out[t] = {"time": t, "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
                  "volume": float(k[5] or 0)}
    return [out[t] for t in sorted(out)]


def open_interest(sym, interval, limit):
    m = {"1": "5min", "3": "5min", "5": "5min", "15": "15min", "30": "30min", "60": "1h", "240": "4h", "D": "1d"}
    r = api(SESSION.get_open_interest, category=CFG["category"], symbol=sym,
            intervalTime=m.get(str(interval), "5min"), limit=limit)
    pts = [{"time": int(x["timestamp"]) // 1000, "value": float(x.get("openInterest") or 0)} for x in r["result"]["list"]]
    return sorted(pts, key=lambda p: p["time"])


def position(sym):
    r = api(SESSION.get_positions, category=CFG["category"], symbol=sym)
    for p in r["result"]["list"]:
        if float(p.get("size") or 0) > 0:
            return {"side": p["side"], "size": float(p["size"]), "avg": float(p.get("avgPrice") or 0),
                    "upnl": float(p.get("unrealisedPnl") or 0)}
    return {"side": None, "size": 0.0, "avg": 0.0, "upnl": 0.0}


def open_orders(sym):
    return api(SESSION.get_open_orders, category=CFG["category"], symbol=sym, limit=50)["result"]["list"]


def all_positions():
    r = api(SESSION.get_positions, category=CFG["category"], settleCoin="USDT", limit=200)
    return [p for p in r["result"]["list"] if abs(float(p.get("size") or 0)) > 0]


def all_open_orders():
    out, cursor = [], None
    for _ in range(10):
        kw = {"category": CFG["category"], "settleCoin": "USDT", "limit": 50}
        if cursor:
            kw["cursor"] = cursor
        r = api(SESSION.get_open_orders, **kw)["result"]
        out += r["list"]
        cursor = r.get("nextPageCursor")
        if not cursor or len(r["list"]) < 50:
            break
    return out


def ours(o, prefix="sg_"):
    return (o.get("orderLinkId") or "").startswith(prefix)


def link(prefix):
    return f"{prefix}{int(time.time() * 1000)}_{int.from_bytes(os.urandom(2), 'big') % 1000}"


def cancel_ours(sym):
    try:
        n = 0
        for o in open_orders(sym):
            if ours(o):
                try:
                    api(SESSION.cancel_order, category=CFG["category"], symbol=sym, orderId=o["orderId"])
                    n += 1
                except Exception:  # noqa: BLE001
                    pass
        if n:
            log.info("%s: снято ордеров: %d", sym, n)
        return n
    except Exception:  # noqa: BLE001
        return 0


def close_market(sym):
    pos = position(sym)
    if not pos["size"]:
        return
    side = "Sell" if pos["side"] == "Buy" else "Buy"
    api(SESSION.place_order, category=CFG["category"], symbol=sym, side=side, orderType="Market",
        qty=fmt_qty(pos["size"], instrument(sym)["qty_step"]), reduceOnly=True, positionIdx=0,
        orderLinkId=link("sg_close_"))
    log.info("%s: закрыта market", sym)


def place_maker_exit(sym, pos, tp_price, inst):
    if not pos["size"]:
        return
    side = "Sell" if pos["side"] == "Buy" else "Buy"
    body = dict(category=CFG["category"], symbol=sym, side=side, orderType="Limit",
                qty=fmt_qty(pos["size"], inst["qty_step"]), price=str(round_step(tp_price, inst["tick"])),
                timeInForce="PostOnly", reduceOnly=True, positionIdx=0, orderLinkId=link("sg_tp_"))
    try:
        api(SESSION.place_order, **body)
    except Exception:  # noqa: BLE001
        body.update(timeInForce="GTC", orderLinkId=link("sg_tp_") + "_g")
        try:
            api(SESSION.place_order, **body)
        except Exception as e:  # noqa: BLE001
            log.warning("%s: Maker TP fail: %s", sym, e)
            return
    log.info("%s: Maker TP @ %s", sym, body["price"])


def place_sell_limit(sym, qty, price, tp, sl, inst):
    body = dict(category=CFG["category"], symbol=sym, side="Sell", orderType="Limit",
                qty=fmt_qty(qty, inst["qty_step"]), price=str(round_step(price, inst["tick"])),
                timeInForce="GTC", positionIdx=0, orderLinkId=link("sg_s_"))
    if sl:
        body.update(stopLoss=str(sl), slTriggerBy="LastPrice")
    if tp and not CFG["maker_exit"]:
        body.update(takeProfit=str(tp), tpTriggerBy="LastPrice")
    try:
        r = api(SESSION.place_order, **body)
        log.info("%s: ✅ Sell %s @ %s #%s", sym, body["qty"], body["price"], r["result"].get("orderId"))
        if sym not in S["own"]:
            S["own"].append(sym)
            save_state()
    except Exception as e:  # noqa: BLE001
        log.warning("%s: ❌ Sell %s @ %s: %s", sym, body["qty"], body["price"], e)


# ---------------------------------------------------------------- индикаторы (как на странице)

def atr_pct(c, period=14):
    if not c or len(c) < period + 1:
        return None
    s = 0.0
    for i in range(len(c) - period, len(c)):
        x, p = c[i], c[i - 1]
        s += max(x["high"] - x["low"], abs(x["high"] - p["close"]), abs(x["low"] - p["close"]))
    last = c[-1]["close"]
    return (s / period) / last if last else None


def rsi(c, period=14):
    if not c or len(c) < period + 1:
        return None
    g = l = 0.0
    for i in range(len(c) - period, len(c)):
        d = c[i]["close"] - c[i - 1]["close"]
        if d >= 0:
            g += d
        else:
            l -= d
    if l == 0:
        return 100.0
    return 100 - 100 / (1 + (g / period) / (l / period))


def cvd(c):
    cum, out = 0.0, []
    for x in c:
        cum += x["volume"] if x["close"] >= x["open"] else -x["volume"]
        out.append({"time": x["time"], "value": cum})
    return out


def shukin_context(c, oi):
    """Правила Щукина для шорта: OI + CVD + RSI/памп. Тот же расчёт, что analyzeShukinContext на странице."""
    res = {"allow": True, "reason": "OK", "oi_trend": "—", "cvd_trend": "—", "oi_chg": 0.0, "cvd_slope": 0.0}
    if not c or len(c) < 10:
        return {**res, "allow": False, "reason": "мало данных"}
    cv = cvd(c)
    avg = lambda a: sum(x["value"] for x in a) / (len(a) or 1)  # noqa: E731
    oi_trend, oi_chg = "n/a", 0.0
    if oi and len(oi) >= 6:
        a_r, a_p = avg(oi[-4:]), avg(oi[-8:-4])
        oi_chg = (a_r - a_p) / a_p * 100 if a_p > 0 else 0.0
        oi_trend = "растёт" if oi_chg > 1.5 else "падает" if oi_chg < -1.0 else "флэт"
    res.update(oi_trend=oi_trend, oi_chg=oi_chg)
    tail = cv[-8:]
    slope = 0.0
    if len(tail) >= 4:
        m = len(tail) // 2
        a1, a2 = avg(tail[:m]), avg(tail[m:])
        slope = (a2 - a1) / (abs(a2) if abs(a2) > 1e-9 else 1)
    res["cvd_slope"] = slope
    res["cvd_trend"] = "покупки" if slope > 0.03 else "продажи" if slope < -0.03 else "нейтр."
    r = rsi(c, 14)
    ago = c[max(0, len(c) - 12)]
    pump = (c[-1]["close"] - ago["close"]) / ago["close"] * 100 if ago["close"] else 0
    if oi_trend == "растёт" and oi_chg > 2.5:
        res.update(allow=False, reason="OI растёт — рано шортить")
    elif slope > 0.08 and oi_trend == "растёт":
        res.update(allow=False, reason="OI↑ + CVD покупки — сила лонга")
    elif slope > 0.12:
        res.update(allow=False, reason="CVD сильные покупки")
    elif oi_trend == "падает" or slope < -0.02:
        res.update(reason="OI падает — деньги уходят" if oi_trend == "падает" else "CVD продажи")
    elif pump > 3 and oi_trend in ("флэт", "n/a") and slope <= 0.05:
        res.update(reason="памп выдыхается")
    elif r is not None and r >= 70 and slope <= 0.04:
        res.update(reason="RSI перекуп + CVD не тянет")
    else:
        res.update(reason="нейтральный контекст")
    return res


# ---------------------------------------------------------------- сканеры (как кнопки на странице)

def tickers():
    return api(SESSION.get_tickers, category=CFG["category"])["result"]["list"]


def scan_pump():
    lst = tickers()
    pumps = [t for t in lst if t["symbol"].endswith("USDT") and float(t.get("turnover24h") or 0) > 2e6
             and float(t.get("price24hPcnt") or 0) >= 0.012]
    if not pumps:
        pumps = [t for t in lst if t["symbol"].endswith("USDT") and float(t.get("turnover24h") or 0) > 1e6]
    pumps.sort(key=lambda t: float(t.get("price24hPcnt") or 0), reverse=True)
    found = []
    for t in pumps[:12]:
        try:
            c = klines(t["symbol"], CFG["interval"], 30)
        except Exception:  # noqa: BLE001
            continue
        r = rsi(c, 14)
        chg = float(t.get("price24hPcnt") or 0) * 100
        if r is None or r >= 58 or chg >= 3:
            found.append({"symbol": t["symbol"], "chg": chg, "rsi": r, "turnover": float(t.get("turnover24h") or 0)})
    found.sort(key=lambda x: (-(x["rsi"] if x["rsi"] is not None else 50), -x["chg"]))
    return found[:10]


def _range(c, n):
    tail = c[-max(1, n):]
    hi, lo = max(x["high"] for x in tail), min(x["low"] for x in tail)
    last = tail[-1]["close"]
    return (hi - lo) / last * 100 if last else 0


def _move(c, n):
    if len(c) < 2:
        return 0
    p = c[max(0, len(c) - 1 - n)]["close"]
    return (c[-1]["close"] - p) / p * 100 if p else 0


def _vol_spike(c, lb):
    if len(c) < lb + 2:
        return 1
    prev = c[-lb - 1:-1]
    a = sum(x["volume"] for x in prev) / (len(prev) or 1)
    return c[-1]["volume"] / a if a > 0 else 1


def scan_ultra():
    pool = [t for t in tickers() if t["symbol"].endswith("USDT") and float(t.get("turnover24h") or 0) > 1.5e6]
    pool.sort(key=lambda t: abs(float(t.get("price24hPcnt") or 0)), reverse=True)

    def score(t):
        try:
            c = klines(t["symbol"], "1", 40)
        except Exception:  # noqa: BLE001
            return None
        if len(c) < 16:
            return None
        r15, r5, r1 = _range(c, 15), _range(c, 5), _range(c, 1)
        m5, m1 = _move(c, 5), _move(c, 1)
        a = (atr_pct(c, 14) or 0) * 100
        sp = _vol_spike(c, 12)
        if not (r15 >= 0.70 or r5 >= 0.45 or r1 >= 0.28 or abs(m5) >= 0.40 or a >= 0.22):
            return None
        sc = r15 * 1.4 + r5 * 1.8 + r1 * 2.2 + abs(m5) * 1.2 + abs(m1) * 1.5 + a * 1.6 + min(sp, 6) * 0.35
        return {"symbol": t["symbol"], "chg": m5, "rsi": rsi(c, 14), "range15": r15, "atr": a, "score": sc}

    with ThreadPoolExecutor(8) as ex:
        res = [x for x in ex.map(score, pool[:48]) if x]
    res.sort(key=lambda x: -x["score"])
    return res[:12]


def maybe_scan(busy_syms):
    """Сканер выбирает монету, если сейчас не во что ставить сетку (как «Найти пампы» → первая цель)."""
    if CFG["symbol"]:
        S["active"] = [CFG["symbol"]]
        return
    if CFG["scan_mode"] == "off":
        return
    keep = [s for s in S["active"] if WATCH.get(s, {}).get("pos", {}).get("size") or WATCH.get(s, {}).get("our_orders")]
    due = time.time() - S["last_scan"] >= CFG["scan_every_min"] * 60
    if not due and len(S["active"]) >= SIZE["coins"]:
        return
    if due or not S["targets"]:
        try:
            found = scan_ultra() if CFG["scan_mode"] == "ultra" else scan_pump()
        except Exception as e:  # noqa: BLE001
            log.warning("сканер: %s", e)
            return
        S["last_scan"] = time.time()
        S["targets"] = found
        new = list(keep)
    else:
        found = S["targets"]                    # депозита хватает на больше монет — добираю из последнего скана
        new = list(S["active"])
    for t in found:
        if len(new) >= SIZE["coins"]:
            break
        if t["symbol"] in new or t["symbol"] in busy_syms:      # монета другого бота на счёте — не трогаю
            continue
        new.append(t["symbol"])
    if new != S["active"]:
        log.info("сканер (%s): цели %s → работаю с %s", CFG["scan_mode"],
                 ", ".join(t["symbol"] for t in found[:5]) or "нет", ", ".join(new) or "нет")
    # монеты, ушедшие из активных, без позиции: снимаю свои ордера
    for s in S["active"]:
        if s not in new and CFG["live"]:
            cancel_ours(s)
    S["active"] = new


# ---------------------------------------------------------------- цикл сетки (runSellGridCycle)

def watch(sym):
    if sym not in WATCH:
        WATCH[sym] = {"symbol": sym, "mid": 0.0, "candles": [], "price_hist": [], "oi": [],
                      "pos": {"side": None, "size": 0.0, "avg": 0.0, "upnl": 0.0}, "orders": 0, "our_orders": 0,
                      "spike": False, "vol_ok": True, "atr": None, "rsi": None, "status": "—", "sig": None}
    return WATCH[sym]


SIZE = {"budget": 0.0, "per_coin": 0.0, "level_usdt": 0.0, "coins": 1, "free": 0.0, "t": 0.0}
LEV_SET = set()


def update_sizing():
    """Максимум депозита: (свободный баланс + маржа, уже занятая ботом) × DEPOSIT_PCT делится на монеты.
    Монет — сколько влезает, чтобы уровень сетки был не меньше MIN_LEVEL_USDT (и не больше MAX_ACTIVE)."""
    c = CFG
    if c["sizing"] != "auto":
        SIZE.update(coins=c["max_active"], level_usdt=c["order_usdt"],
                    per_coin=c["order_usdt"] * c["grid_levels"] / max(c["leverage"], 1))
        return
    if not c["live"]:
        free = 100.0
    else:
        if time.time() - SIZE["t"] < 15:
            return
        r = api(SESSION.get_wallet_balance, accountType="UNIFIED")["result"]["list"][0]
        free = float(r.get("totalAvailableBalance") or 0)
    mine = 0.0                              # маржа, которую бот уже занял (позиции + свои sell-лимитки)
    for w in WATCH.values():
        if w["symbol"] not in S["own"]:
            continue
        p = w["pos"]
        mine += (p["size"] * (p["avg"] or w["mid"]) + w.get("sell_notional", 0.0)) / max(c["leverage"], 1)
    budget = (free + mine) * c["deposit_pct"] / 100
    lev, lv = max(c["leverage"], 1), max(int(c["grid_levels"]), 1)
    coins = int(budget * lev // (lv * c["min_level_usdt"])) if budget > 0 else 0
    coins = max(1, min(c["max_active"], coins))
    per = budget / coins
    SIZE.update(budget=budget, per_coin=per, level_usdt=per * lev / lv, coins=coins, free=free, t=time.time())


def ensure_leverage(sym):
    if sym in LEV_SET or not CFG["live"]:
        return
    lev = str(CFG["leverage"]).rstrip("0").rstrip(".")
    try:
        api(SESSION.set_leverage, category=CFG["category"], symbol=sym, buyLeverage=lev, sellLeverage=lev)
        log.info("%s: плечо %sx", sym, lev)
    except Exception as e:  # noqa: BLE001
        if "not modified" not in str(e).lower() and "110043" not in str(e):
            log.warning("%s: плечо не поставилось (%s) — работаю с текущим", sym, e)
    LEV_SET.add(sym)


def sell_grid_cycle(sym, w, mid, inst, vol_ok, spike, entry_allowed):
    c = CFG
    step, tp, sl = c["grid_step_pct"], c["tp_pct"], c["sl_pct"]
    if c["shukin"]:                      # Щукин: шаг от 1%, тейк от 0.7%, аварийный SL 10%
        step, tp, sl = max(step, 1.0), max(tp, 0.7), max(sl, 10.0)
    step, tp, sl = step / 100, tp / 100, sl / 100
    if SIZE["level_usdt"] <= 0:
        w["status"] = "жду расчёт размера"
        return
    lvl = max(SIZE["level_usdt"], 5.2)                       # Bybit не берёт ордера меньше 5$
    st = Decimal(str(inst["qty_step"]))
    qty = float((Decimal(str(lvl / mid)) / st).to_integral_value(ROUND_CEILING) * st)
    qty = max(qty, float(inst["min_qty"]))
    # максимум позиции + лимиток на монету, $ (запас — уровни стоят выше цены, на шаг × уровни дороже)
    cap = SIZE["per_coin"] * max(c["leverage"], 1) * (1 + step * int(c["grid_levels"]))
    targets = [round_step(mid * (1 + step * i), inst["tick"]) for i in range(1, int(c["grid_levels"]) + 1)]
    w["targets"] = targets
    if not c["live"]:
        w["status"] = "SIM"
        return

    pos = w["pos"]
    if pos["size"] > 0 and w["our_orders"]:
        # позиция выросла: свои sell сверх лимита монеты снимаю (сначала самые дальние) — даже если фильтр «ждать»
        sl_orders = sorted([o for o in open_orders(sym) if ours(o, "sg_s_")], key=lambda o: float(o.get("price") or 0))
        used = pos["size"] * (pos["avg"] or mid)
        for o in sl_orders:
            n = float(o.get("qty") or 0) * float(o.get("price") or 0)
            if used + n > cap:
                try:
                    api(SESSION.cancel_order, category=c["category"], symbol=sym, orderId=o["orderId"])
                    log.info("%s: снят @ %s — лимит депозита на монету %.0f$", sym, o.get("price"), cap)
                except Exception:  # noqa: BLE001
                    pass
            else:
                used += n
    if pos["size"] > 0 and pos["side"] == "Sell":
        entry = pos["avg"] or mid
        move = (entry - mid) / entry
        if move >= tp or move <= -sl:
            what = "TP" if move >= tp else "SL"
            log.info("%s: %s %.2f%%%s", sym, "🎯 " + what if what == "TP" else "🚨 " + what, move * 100,
                     " (Щукин)" if c["shukin"] else "")
            pnl = pos["upnl"]
            close_market(sym)
            cancel_ours(sym)
            record_trade(sym, pnl, what)
            return
        if c["maker_exit"]:
            tps = [o for o in open_orders(sym) if ours(o, "sg_tp_")]
            if tps and abs(float(tps[0].get("qty") or 0) - pos["size"]) > 1e-12:
                # позиция подросла (добор) — переставляю тейк на весь объём
                for o in tps:
                    try:
                        api(SESSION.cancel_order, category=c["category"], symbol=sym, orderId=o["orderId"])
                    except Exception:  # noqa: BLE001
                        pass
                tps = []
            if not tps:
                place_maker_exit(sym, pos, entry * (1 - tp), inst)
        if not c["shukin"]:              # обычный режим: в позиции новых входов нет
            return
    elif pos["size"] > 0:
        w["status"] = "LONG — не моя"
        return

    if not entry_allowed:
        if not pos["size"] and w["our_orders"]:
            cancel_ours(sym)            # монета больше не в работе — свои лимитки снимаю
        w["status"] = "без новых входов"
        return
    if spike or not vol_ok:
        w["status"] = "SPIKE PAUSE" if spike else "VOL FILTER"
        return
    if c["shukin"] and c["shukin_filter"]:
        if S["cycles"] % 5 == 0 or not w["oi"]:
            try:
                w["oi"] = open_interest(sym, c["interval"], 50)
            except Exception:  # noqa: BLE001
                pass
        sig = shukin_context(w["candles"], w["oi"])
        w["sig"] = sig
        if not sig["allow"]:
            w["status"] = "ЩУКИН ЖДАТЬ: " + sig["reason"]
            return

    sells = [o for o in open_orders(sym) if ours(o, "sg_s_") and o["side"] == "Sell"]
    ensure_leverage(sym)
    tol = mid * step * 0.35
    matched, cancel = set(), []
    for o in sells:
        px = float(o.get("price") or 0)
        hit = next((t for t in targets if px and abs(px - t) <= tol), None)
        if hit is not None and hit not in matched:
            matched.add(hit)
        else:
            cancel.append(o)
    for o in cancel:
        try:
            api(SESSION.cancel_order, category=c["category"], symbol=sym, orderId=o["orderId"])
            log.info("%s: снят лишний @ %s", sym, o.get("price"))
        except Exception:  # noqa: BLE001
            pass
    kept = [o for o in sells if o not in cancel]
    used = pos["size"] * (pos["avg"] or mid) + sum(float(o.get("qty") or 0) * float(o.get("price") or 0) for o in kept)
    placed = 0
    for px in targets:
        if px in matched:
            continue
        if used + qty * px > cap:
            if not placed:
                w["status"] = f"лимит депозита на монету ({cap:.0f}$)"
            break
        used += qty * px
        placed += 1
        tpp = round_step(px * (1 - tp), inst["tick"])
        slp = round_step(px * (1 + sl), inst["tick"]) if (not c["shukin"] or pos["size"] == 0) else None
        place_sell_limit(sym, qty, px, tpp, slp, inst)
    w["sell_notional"] = used - pos["size"] * (pos["avg"] or mid)
    if placed or matched:
        w["status"] = "GRID"


def process(sym, entry_allowed):
    """processSymbol: цена, спайк, свечи, ATR/RSI, позиция и цикл сетки по одной монете."""
    w = watch(sym)
    b, a = orderbook(sym)
    if not b or not a:
        raise RuntimeError("пустой стакан")
    mid = (b[0] + a[0]) / 2
    w["mid"] = mid
    inst = instrument(sym)
    w["price_hist"] = (w["price_hist"] + [mid])[-5:]
    spike = False
    if CFG["spike_protect"] and len(w["price_hist"]) >= 3:
        old = w["price_hist"][0]
        spike = abs(mid - old) / old >= CFG["max_spike_pct"] / 100
    w["spike"] = spike
    if not w["candles"] or S["cycles"] % 3 == 0:
        try:
            w["candles"] = klines(sym, CFG["interval"], 60)
        except Exception:  # noqa: BLE001
            pass
    w["atr"], w["rsi"] = atr_pct(w["candles"], 14), rsi(w["candles"], 14)
    vol_ok = True
    if CFG["vol_filter"] and w["atr"] is not None:
        vol_ok = CFG["min_atr_pct"] / 100 <= w["atr"] <= CFG["max_atr_pct"] / 100
    w["vol_ok"] = vol_ok
    w["status"] = "SPIKE" if spike else ("VOL" if not vol_ok else "OK")
    if CFG["live"]:
        w["pos"] = position(sym)
        oo = open_orders(sym)
        w["orders"], w["our_orders"] = len(oo), sum(1 for o in oo if ours(o))
    # новые sell: на монете сканера, а в режиме Щукина — и добор в уже открытый шорт
    short = w["pos"]["size"] > 0 and w["pos"]["side"] == "Sell"
    allowed = entry_allowed and (sym in S["active"] or CFG["symbol"] == sym or (CFG["shukin"] and short))
    sell_grid_cycle(sym, w, mid, inst, vol_ok, spike, allowed)
    st = w["status"]
    if st != w.get("_logged"):          # в лог — только смена статуса (почему ставлю / не ставлю сетку)
        w["_logged"] = st
        extra = ""
        if w.get("atr") is not None:
            extra = f" (ATR {w['atr'] * 100:.2f}%, RSI {w['rsi'] or 0:.0f}"
            if w.get("sig"):
                extra += f", OI {w['sig']['oi_trend']} {w['sig']['oi_chg']:+.1f}%, CVD {w['sig']['cvd_trend']}"
            extra += ")"
        log.info("%s: статус → %s%s", sym, st, extra)


def universe():
    """Какие монеты контролирую: свои (где ставил ордера) + активные от сканера.
    Монеты с чужими позициями/ордерами (другие боты на счёте) — не трогаю (если не MANAGE_ALL)."""
    busy = set()
    if not CFG["live"]:
        return list(dict.fromkeys(S["active"])), busy
    pos, orders = all_positions(), all_open_orders()
    have_pos = {p["symbol"] for p in pos}
    our_ord = {o["symbol"] for o in orders if ours(o)}
    foreign = {o["symbol"] for o in orders if not ours(o)}
    if CFG["manage_all"]:
        syms = have_pos | {o["symbol"] for o in orders} | set(S["active"])
        return sorted(syms), busy
    own = [s for s in S["own"] if s in have_pos or s in our_ord or s in S["active"]]
    if own != S["own"]:
        S["own"] = own
        save_state()
    busy = (have_pos | foreign) - set(own)
    return list(dict.fromkeys(own + S["active"])), busy


def cycle():
    ensure_day()
    if web.CONTROL["paused"]:
        if CFG["live"] and not S.get("pause_done"):
            # как «Стоп» на странице: снимаю свои sell-лимитки; позиции и их тейки остаются
            for sym in list(WATCH):
                try:
                    for o in open_orders(sym):
                        if ours(o, "sg_s_"):
                            api(SESSION.cancel_order, category=CFG["category"], symbol=sym, orderId=o["orderId"])
                except Exception as e:  # noqa: BLE001
                    log.warning("%s: стоп: %s", sym, e)
            log.info("Стоп: sell-лимитки сняты, открытые позиции остаются со своими тейками/стопами")
        S["pause_done"] = True
        web.update(state="на паузе — нажми «Старт»")
        return
    S["pause_done"] = False
    if CFG["max_daily_loss"] > 0 and S["day_pnl"] <= -abs(CFG["max_daily_loss"]):
        web.update(state=f"DAILY LIMIT: день {S['day_pnl']:+.2f}$ — новые входы до 00:00 UTC")
        if CFG["live"]:
            for s in list(WATCH):
                if not WATCH[s]["pos"]["size"]:
                    cancel_ours(s)
        entry_allowed = False
    else:
        entry_allowed = True
    syms, busy = universe()
    try:
        update_sizing()
    except Exception as e:  # noqa: BLE001
        log.warning("баланс для расчёта размера: %s", e)
    if entry_allowed:
        maybe_scan(busy)
        syms, busy = universe()
    for s in list(WATCH):
        if s not in syms:
            del WATCH[s]
    if not syms:
        web.update(state="жду цель от сканера")
    errs = 0
    with ThreadPoolExecutor(max(1, min(8, len(syms) or 1))) as ex:
        futs = {ex.submit(process, s, entry_allowed): s for s in syms}
        for f, s in futs.items():
            try:
                f.result()
            except Exception as e:  # noqa: BLE001
                errs += 1
                log.warning("%s: %s", s, e)
                watch(s)["status"] = "ERR: " + str(e)[:80]
    S["cycles"] += 1
    if entry_allowed and syms:
        n_pos = sum(1 for s in syms if WATCH.get(s, {}).get("pos", {}).get("size"))
        web.update(state=("RUN ЩУКИН" if CFG["shukin"] else "RUN GRID") + f" · {len(syms)} пар"
                   + f" · депозит {SIZE['budget']:.0f}$ → {SIZE['coins']} мон. × {SIZE['level_usdt']:.1f}$/ур."
                   + (f" · POS×{n_pos}" if n_pos else "") + (" · SIM (LIVE=0)" if not CFG["live"] else ""))


def snapshot():
    with LOCK:
        return {"cfg": CFG, "s": {k: S[k] for k in ("cycles", "trades", "wins", "losses", "day_pnl", "targets",
                                                    "active", "history", "own")},
                "size": dict(SIZE),
                "watch": {k: {kk: v for kk, v in w.items() if kk not in ("candles", "price_hist", "oi")}
                          for k, w in WATCH.items()}}


def main():
    global SESSION, HAS_KEYS
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S", stream=sys.stdout, force=True)
    web.SNAPSHOT = snapshot
    web.FILTERS = FILTERS
    web.SET_FILTER = set_filter
    web.start()
    load_state()
    SESSION, HAS_KEYS = make_session()
    if CFG["live"] and not HAS_KEYS:
        log.error("LIVE=1, но нет BYBIT_API_KEY / BYBIT_API_SECRET — работаю в симуляции")
        CFG["live"] = False
    log.info("Sell Grid %s | %s | шаг %.2f%% × %d ур., тейк %.2f%%, стоп %.2f%%, %.0f$ на уровень, лимит дня %.0f$, "
             "сканер %s%s", "ЩУКИН" if CFG["shukin"] else "обычный",
             ("LIVE" if CFG["live"] else "SIM") + (" testnet" if CFG["testnet"] else ""), CFG["grid_step_pct"],
             CFG["grid_levels"], CFG["tp_pct"], CFG["sl_pct"], CFG["order_usdt"], CFG["max_daily_loss"],
             CFG["scan_mode"], f", монета {CFG['symbol']}" if CFG["symbol"] else "")
    log.info("Размер: %s", f"авто — {CFG['deposit_pct']:g}% свободного баланса, плечо {CFG['leverage']:g}x, до "
             f"{CFG['max_active']} монет, уровень ≥ {CFG['min_level_usdt']:g}$" if CFG["sizing"] == "auto"
             else f"фиксированный {CFG['order_usdt']:g}$ на уровень")
    log.info("Фильтры: %s", ", ".join(f"{v} {'ВКЛ' if CFG[k] else 'ВЫКЛ'}" for k, v in FILTERS.items()))
    if CFG["live"]:
        try:
            r = api(SESSION.get_wallet_balance, accountType="UNIFIED", coin="USDT")
            log.info("Ключи OK, баланс USDT: %s", r["result"]["list"][0].get("totalWalletBalance"))
        except Exception as e:  # noqa: BLE001
            log.error("Ключи не работают: %s", e)
            web.update(error=f"Ключи не работают: {e}")
    while True:
        t0 = time.time()
        try:
            with LOCK:
                cycle()
            web.update(error="")
        except Exception as e:  # noqa: BLE001
            log.error("Ошибка цикла: %s", e)
            web.update(error=str(e)[:300])
        time.sleep(max(0.5, CFG["cycle_sec"] - (time.time() - t0)))


if __name__ == "__main__":
    main()
