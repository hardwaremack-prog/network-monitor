import json
import os
import socket
import sqlite3
import sys
import threading
import time
import platform
import webbrowser
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen, Request

try:
    import psutil
except ImportError:
    print("This tool needs psutil. Install it with:\n\n    pip install psutil\n")
    sys.exit(1)

PORT = 8420
HIST_LEN = 60
SAMPLE_INTERVAL = 1.0
USAGE_LOG_INTERVAL = 60
LATENCY_INTERVAL = 3
OUTAGE_FAIL_THRESHOLD = 2          # consecutive failed pings before declaring an outage
SPEEDTEST_INTERVAL = 15 * 60       # auto speed test cadence
SPEEDTEST_DOWNLOAD_BYTES = 10_000_000
SPEEDTEST_UPLOAD_BYTES = 4_000_000

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "network_monitor.db")

lock = threading.Lock()
db_lock = threading.Lock()

STATE = {
    "started": time.time(),
    "public_ip": None,
    "latency_ms": None,
    "internet_up": True,
    "fail_streak": 0,
    "current_outage_start": None,
    "adapters": {},
}

speedtest_state = {"running": False, "last_result": None, "last_error": None}


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def db_init():
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""CREATE TABLE IF NOT EXISTS outages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_ts REAL, end_ts REAL, duration_s REAL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS usage_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL, down_bytes INTEGER, up_bytes INTEGER
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS speed_tests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL, download_mbps REAL, upload_mbps REAL, ping_ms REAL
        )""")
        conn.commit()
        conn.close()


def db_exec(query, params=(), fetch=False, commit=False):
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.execute(query, params)
        result = cur.fetchall() if fetch else None
        if commit:
            conn.commit()
        conn.close()
        return result


# ---------------------------------------------------------------------------
# Adapter kind / duplex helpers
# ---------------------------------------------------------------------------

def guess_kind(name):
    n = name.lower()
    if n.startswith("lo"):
        return "loopback"
    if any(k in n for k in ("wi-fi", "wifi", "wlan", "wl")):
        return "wifi"
    if any(k in n for k in ("docker", "veth", "br-", "virbr", "vmnet", "vbox", "hyper-v", "vethernet")):
        return "virtual"
    if any(k in n for k in ("tun", "tap", "wg", "ppp", "utun", "ipsec", "vpn")):
        return "vpn"
    if any(k in n for k in ("eth", "en", "enp", "eno")):
        return "ethernet"
    return "other"


def duplex_name(d):
    try:
        if d == psutil.NIC_DUPLEX_FULL:
            return "full"
        if d == psutil.NIC_DUPLEX_HALF:
            return "half"
    except Exception:
        pass
    return "unknown"


def mac_and_ip(name, addrs):
    mac, ipv4 = "-", None
    for a in addrs.get(name, []):
        fam = a.family
        if fam == psutil.AF_LINK and a.address:
            mac = a.address
        elif fam == socket.AF_INET:
            ipv4 = a.address
    return mac, ipv4


# ---------------------------------------------------------------------------
# Background: per-adapter sampler (1s)
# ---------------------------------------------------------------------------

def sampler():
    prev_counters = psutil.net_io_counters(pernic=True)
    prev_time = time.time()

    while True:
        time.sleep(SAMPLE_INTERVAL)
        now = time.time()
        elapsed = max(now - prev_time, 0.001)
        counters = psutil.net_io_counters(pernic=True)
        stats = psutil.net_if_stats()
        addrs = psutil.net_if_addrs()

        with lock:
            for name, c in counters.items():
                st = STATE["adapters"].setdefault(name, {
                    "hist_down": deque([0.0] * HIST_LEN, maxlen=HIST_LEN),
                    "hist_up": deque([0.0] * HIST_LEN, maxlen=HIST_LEN),
                    "first_down": c.bytes_recv,
                    "first_up": c.bytes_sent,
                })
                p = prev_counters.get(name, c)
                down_mbps = max(0.0, (c.bytes_recv - p.bytes_recv) * 8 / 1e6 / elapsed)
                up_mbps = max(0.0, (c.bytes_sent - p.bytes_sent) * 8 / 1e6 / elapsed)
                packets_ps = max(0.0, ((c.packets_recv + c.packets_sent) -
                                        (p.packets_recv + p.packets_sent)) / elapsed)

                st["hist_down"].append(down_mbps)
                st["hist_up"].append(up_mbps)
                st["down_mbps"] = down_mbps
                st["up_mbps"] = up_mbps
                st["packets_ps"] = packets_ps
                st["total_down_gb"] = (c.bytes_recv - st["first_down"]) / (1024 ** 3)
                st["total_up_gb"] = (c.bytes_sent - st["first_up"]) / (1024 ** 3)
                st["errors"] = c.errin + c.errout + c.dropin + c.dropout

                nstat = stats.get(name)
                mac, ipv4 = mac_and_ip(name, addrs)
                st["name"] = name
                st["kind"] = guess_kind(name)
                st["isup"] = bool(nstat.isup) if nstat else False
                st["speed_mbps"] = nstat.speed if nstat else 0
                st["mtu"] = nstat.mtu if nstat else 0
                st["duplex"] = duplex_name(nstat.duplex) if nstat else "unknown"
                st["mac"] = mac
                st["ipv4"] = ipv4

            for name in list(STATE["adapters"].keys()):
                if name not in counters:
                    del STATE["adapters"][name]

        prev_counters = counters
        prev_time = now


# ---------------------------------------------------------------------------
# Background: latency probe + outage detection (3s)
# ---------------------------------------------------------------------------

def latency_probe():
    while True:
        ok = False
        ms = None
        try:
            t0 = time.time()
            s = socket.create_connection(("1.1.1.1", 53), timeout=1.5)
            s.close()
            ms = round((time.time() - t0) * 1000, 1)
            ok = True
        except Exception:
            ok = False

        with lock:
            STATE["latency_ms"] = ms if ok else None
            if ok:
                STATE["fail_streak"] = 0
                if not STATE["internet_up"] and STATE["current_outage_start"] is not None:
                    end_ts = time.time()
                    start_ts = STATE["current_outage_start"]
                    duration = end_ts - start_ts
                    db_exec(
                        "UPDATE outages SET end_ts=?, duration_s=? WHERE start_ts=? AND end_ts IS NULL",
                        (end_ts, duration, start_ts), commit=True,
                    )
                    STATE["current_outage_start"] = None
                STATE["internet_up"] = True
            else:
                STATE["fail_streak"] += 1
                if STATE["internet_up"] and STATE["fail_streak"] >= OUTAGE_FAIL_THRESHOLD:
                    STATE["internet_up"] = False
                    start_ts = time.time()
                    STATE["current_outage_start"] = start_ts
                    db_exec(
                        "INSERT INTO outages (start_ts, end_ts, duration_s) VALUES (?, NULL, NULL)",
                        (start_ts,), commit=True,
                    )

        time.sleep(LATENCY_INTERVAL)


# ---------------------------------------------------------------------------
# Background: public IP (60s)
# ---------------------------------------------------------------------------

def public_ip_probe():
    while True:
        try:
            with urlopen("https://api.ipify.org?format=json", timeout=4) as r:
                data = json.loads(r.read().decode())
                with lock:
                    STATE["public_ip"] = data.get("ip")
        except Exception:
            with lock:
                STATE["public_ip"] = None
        time.sleep(60)


# ---------------------------------------------------------------------------
# Background: usage history logger (60s) -> persisted rollup
# ---------------------------------------------------------------------------

def usage_logger():
    prev = psutil.net_io_counters()
    while True:
        time.sleep(USAGE_LOG_INTERVAL)
        cur = psutil.net_io_counters()
        down_delta = max(0, cur.bytes_recv - prev.bytes_recv)
        up_delta = max(0, cur.bytes_sent - prev.bytes_sent)
        db_exec(
            "INSERT INTO usage_snapshots (ts, down_bytes, up_bytes) VALUES (?, ?, ?)",
            (time.time(), down_delta, up_delta), commit=True,
        )
        prev = cur


# ---------------------------------------------------------------------------
# Background: active connections by process (2s, in-memory only)
# ---------------------------------------------------------------------------

connections_state = {"processes": [], "updated": 0}


def connections_sampler():
    while True:
        grouped = {}
        for proc in psutil.process_iter(["pid", "name"]):
            try:
                conns = proc.net_connections(kind="inet") if hasattr(proc, "net_connections") else proc.connections(kind="inet")
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
            if not conns:
                continue
            key = proc.info["name"] or "unknown"
            g = grouped.setdefault(key, {"name": key, "count": 0, "remotes": set(), "statuses": {}})
            for c in conns:
                g["count"] += 1
                if c.raddr:
                    g["remotes"].add("%s:%s" % (c.raddr.ip, c.raddr.port))
                g["statuses"][c.status] = g["statuses"].get(c.status, 0) + 1

        rows = []
        for g in grouped.values():
            rows.append({
                "name": g["name"],
                "count": g["count"],
                "remotes": sorted(g["remotes"])[:5],
                "established": g["statuses"].get("ESTABLISHED", 0),
            })
        rows.sort(key=lambda r: r["count"], reverse=True)

        with lock:
            connections_state["processes"] = rows[:15]
            connections_state["updated"] = time.time()

        time.sleep(2.0)


# ---------------------------------------------------------------------------
# Speed test (Cloudflare's public __down / __up test endpoints)
# ---------------------------------------------------------------------------

def run_speed_test():
    if speedtest_state["running"]:
        return
    speedtest_state["running"] = True
    speedtest_state["last_error"] = None
    try:
        # Ping
        ping_ms = None
        try:
            t0 = time.time()
            s = socket.create_connection(("1.1.1.1", 53), timeout=2)
            s.close()
            ping_ms = round((time.time() - t0) * 1000, 1)
        except Exception:
            pass

        # Download
        down_mbps = None
        try:
            url = "https://speed.cloudflare.com/__down?bytes=%d" % SPEEDTEST_DOWNLOAD_BYTES
            t0 = time.time()
            total = 0
            with urlopen(url, timeout=20) as r:
                while True:
                    chunk = r.read(65536)
                    if not chunk:
                        break
                    total += len(chunk)
                    if time.time() - t0 > 15:
                        break
            elapsed = max(time.time() - t0, 0.001)
            down_mbps = round(total * 8 / 1e6 / elapsed, 2)
        except Exception as e:
            speedtest_state["last_error"] = "download test failed: %s" % e

        # Upload
        up_mbps = None
        try:
            payload = os.urandom(SPEEDTEST_UPLOAD_BYTES)
            req = Request("https://speed.cloudflare.com/__up", data=payload, method="POST")
            t0 = time.time()
            with urlopen(req, timeout=20) as r:
                r.read()
            elapsed = max(time.time() - t0, 0.001)
            up_mbps = round(len(payload) * 8 / 1e6 / elapsed, 2)
        except Exception as e:
            prev = speedtest_state["last_error"]
            msg = "upload test failed: %s" % e
            speedtest_state["last_error"] = (prev + "; " + msg) if prev else msg

        result = {
            "ts": time.time(),
            "download_mbps": down_mbps,
            "upload_mbps": up_mbps,
            "ping_ms": ping_ms,
        }
        speedtest_state["last_result"] = result
        if down_mbps is not None or up_mbps is not None:
            db_exec(
                "INSERT INTO speed_tests (ts, download_mbps, upload_mbps, ping_ms) VALUES (?, ?, ?, ?)",
                (result["ts"], down_mbps, up_mbps, ping_ms), commit=True,
            )
    finally:
        speedtest_state["running"] = False


def speedtest_scheduler():
    time.sleep(10)
    run_speed_test()
    while True:
        time.sleep(SPEEDTEST_INTERVAL)
        run_speed_test()


# ---------------------------------------------------------------------------
# Snapshot builders for the API
# ---------------------------------------------------------------------------

def build_snapshot():
    with lock:
        adapters = []
        for st in STATE["adapters"].values():
            adapters.append({
                "name": st.get("name"), "kind": st.get("kind"), "isup": st.get("isup"),
                "ipv4": st.get("ipv4"), "mac": st.get("mac"),
                "speed_mbps": st.get("speed_mbps", 0), "mtu": st.get("mtu", 0),
                "duplex": st.get("duplex"),
                "down_mbps": round(st.get("down_mbps", 0.0), 3),
                "up_mbps": round(st.get("up_mbps", 0.0), 3),
                "packets_ps": round(st.get("packets_ps", 0.0), 1),
                "total_down_gb": round(st.get("total_down_gb", 0.0), 3),
                "total_up_gb": round(st.get("total_up_gb", 0.0), 3),
                "errors": st.get("errors", 0),
                "hist_down": list(st.get("hist_down", [])),
                "hist_up": list(st.get("hist_up", [])),
            })
        adapters.sort(key=lambda a: (not a["isup"], a["kind"] == "loopback", a["name"]))

        total_down = sum(a["down_mbps"] for a in adapters if a["isup"])
        total_up = sum(a["up_mbps"] for a in adapters if a["isup"])
        active = sum(1 for a in adapters if a["isup"])

        current_outage_s = None
        if STATE["current_outage_start"] is not None:
            current_outage_s = round(time.time() - STATE["current_outage_start"])

        snapshot = {
            "hostname": socket.gethostname(),
            "platform": platform.system() + " " + platform.release(),
            "uptime_seconds": round(time.time() - STATE["started"]),
            "public_ip": STATE["public_ip"],
            "latency_ms": STATE["latency_ms"],
            "internet_up": STATE["internet_up"],
            "current_outage_seconds": current_outage_s,
            "adapters": adapters,
            "totals": {
                "down_mbps": round(total_down, 2), "up_mbps": round(total_up, 2),
                "active": active, "count": len(adapters),
                "packets_ps": round(sum(a["packets_ps"] for a in adapters if a["isup"]), 1),
            },
        }
    return snapshot


def build_outages(limit=20):
    rows = db_exec(
        "SELECT start_ts, end_ts, duration_s FROM outages ORDER BY start_ts DESC LIMIT ?",
        (limit,), fetch=True,
    )
    out = []
    for start_ts, end_ts, duration_s in rows:
        out.append({"start_ts": start_ts, "end_ts": end_ts, "duration_s": duration_s})
    today = time.strftime("%Y-%m-%d")
    today_rows = db_exec(
        "SELECT duration_s, start_ts FROM outages WHERE duration_s IS NOT NULL",
        fetch=True,
    )
    today_count = 0
    today_total = 0.0
    for duration_s, start_ts in today_rows:
        if time.strftime("%Y-%m-%d", time.localtime(start_ts)) == today:
            today_count += 1
            today_total += duration_s or 0
    return {"recent": out, "today_count": today_count, "today_total_s": round(today_total)}


def build_history(days=14):
    rows = db_exec(
        """SELECT date(ts, 'unixepoch', 'localtime') as day,
                  SUM(down_bytes) as d, SUM(up_bytes) as u
           FROM usage_snapshots GROUP BY day ORDER BY day DESC LIMIT ?""",
        (days,), fetch=True,
    )
    daily = [{"day": day, "down_gb": round((d or 0) / (1024 ** 3), 3),
              "up_gb": round((u or 0) / (1024 ** 3), 3)} for day, d, u in rows]
    daily.reverse()
    today = time.strftime("%Y-%m-%d")
    week_ago = time.time() - 7 * 86400
    week_rows = db_exec(
        "SELECT down_bytes, up_bytes FROM usage_snapshots WHERE ts >= ?", (week_ago,), fetch=True,
    )
    week_down = sum(r[0] for r in week_rows) / (1024 ** 3)
    week_up = sum(r[1] for r in week_rows) / (1024 ** 3)
    today_entry = next((d for d in daily if d["day"] == today), {"down_gb": 0, "up_gb": 0})
    return {
        "daily": daily,
        "today_down_gb": round(today_entry["down_gb"], 3),
        "today_up_gb": round(today_entry["up_gb"], 3),
        "week_down_gb": round(week_down, 3),
        "week_up_gb": round(week_up, 3),
    }


def build_speedtests(limit=20):
    rows = db_exec(
        "SELECT ts, download_mbps, upload_mbps, ping_ms FROM speed_tests ORDER BY ts DESC LIMIT ?",
        (limit,), fetch=True,
    )
    tests = [{"ts": ts, "download_mbps": d, "upload_mbps": u, "ping_ms": p} for ts, d, u, p in rows]
    tests.reverse()
    return {
        "history": tests,
        "running": speedtest_state["running"],
        "last_error": speedtest_state["last_error"],
    }


def build_connections():
    with lock:
        return {"processes": list(connections_state["processes"]), "updated": connections_state["updated"]}


# ---------------------------------------------------------------------------
# Frontend (single HTML page, polls the JSON endpoints above)
# ---------------------------------------------------------------------------

PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Network Monitor</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root{
    --bg: #0A0D11; --panel: #10151B; --panel-2: #141B23; --line: #1E2831; --line-soft: #171F27;
    --text: #E7ECF0; --text-dim: #7C8896; --text-faint: #4B5560;
    --down: #4FA8FF; --down-dim: #1E3A54; --up: #FFB454; --up-dim: #4A3820;
    --ok: #38D39F; --ok-dim: #123A2E; --warn: #FF6B6B; --warn-dim: #3A1818;
    --font-mono: 'JetBrains Mono', monospace; --font-sans: 'Space Grotesk', sans-serif;
  }
  *{ box-sizing: border-box; margin:0; padding:0; }
  html,body{ background: var(--bg); color: var(--text); font-family: var(--font-sans); min-height: 100vh; }
  body{ padding: 28px 32px 48px; }
  .top{ display:flex; justify-content: space-between; align-items: flex-start; margin-bottom: 22px; flex-wrap: wrap; gap: 16px; border-bottom: 1px solid var(--line); padding-bottom: 20px; }
  .brand{ display:flex; align-items:center; gap: 12px; }
  .brand .dot{ width: 9px; height: 9px; border-radius: 50%; background: var(--ok); box-shadow: 0 0 0 3px var(--ok-dim); animation: pulse 2.2s ease-in-out infinite; }
  .brand .dot.bad{ background: var(--warn); box-shadow: 0 0 0 3px var(--warn-dim); }
  @keyframes pulse{ 0%,100%{ opacity: 1; } 50%{ opacity: .45; } }
  .brand h1{ font-size: 20px; font-weight: 600; letter-spacing: 0.3px; }
  .brand .eyebrow{ font-family: var(--font-mono); font-size: 10.5px; letter-spacing: 1.5px; color: var(--text-faint); text-transform: uppercase; display:block; margin-bottom: 2px; }
  .clock{ font-family: var(--font-mono); font-size: 13px; color: var(--text-dim); text-align: right; }
  .clock .date{ color: var(--text-faint); font-size: 11px; margin-top: 2px;}
  .summary{ display:grid; grid-template-columns: 1.1fr 1fr 1fr 1fr 1fr; gap: 1px; background: var(--line); border: 1px solid var(--line); border-radius: 10px; overflow: hidden; margin-bottom: 18px; }
  .summary .cell{ background: var(--panel); padding: 16px 18px; position: relative; }
  .summary .cell .label{ font-family: var(--font-mono); font-size: 10px; letter-spacing: 1px; text-transform: uppercase; color: var(--text-faint); margin-bottom: 8px; }
  .summary .cell .value{ font-family: var(--font-mono); font-size: 22px; font-weight: 600; letter-spacing: -0.5px; }
  .summary .cell .value .unit{ font-size: 12px; color: var(--text-dim); font-weight: 400; margin-left: 4px; }
  .summary .cell.down .value{ color: var(--down); }
  .summary .cell.up .value{ color: var(--up); }
  .summary .cell .sub{ font-size: 11px; color: var(--text-faint); margin-top: 4px; font-family: var(--font-mono); }
  .summary .cell.status .value.bad{ color: var(--warn); }
  .summary .cell.status .value.good{ color: var(--ok); }
  .ring-cell{ display:flex; align-items:center; gap: 14px; }
  .ring-cell svg{ flex-shrink: 0; }
  .ring-cell .ring-text .value{ font-size: 19px; }
  .ring-cell .ring-text .label{ margin-bottom: 4px; }
  .grid{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; margin-bottom: 22px; }
  .card{ background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 18px 20px 16px; }
  .card.offline{ opacity: 0.5; }
  .card-head{ display:flex; justify-content: space-between; align-items: flex-start; margin-bottom: 14px; }
  .adapter-name{ display:flex; align-items:center; gap: 9px; }
  .adapter-name .icon{ width: 30px; height:30px; border-radius: 7px; background: var(--panel-2); border: 1px solid var(--line); display:flex; align-items:center; justify-content:center; font-size: 14px; flex-shrink:0; }
  .adapter-name .txt h3{ font-size: 14.5px; font-weight: 600; word-break: break-all; }
  .adapter-name .txt .type{ font-family: var(--font-mono); font-size: 10px; color: var(--text-faint); letter-spacing: .5px; text-transform: uppercase; margin-top: 1px; }
  .status-pill{ font-family: var(--font-mono); font-size: 10px; letter-spacing: .5px; text-transform: uppercase; padding: 3px 8px; border-radius: 20px; display:flex; align-items:center; gap:5px; white-space: nowrap; flex-shrink:0; }
  .status-pill.up{ background: var(--ok-dim); color: var(--ok); }
  .status-pill.down{ background: var(--warn-dim); color: var(--warn); }
  .status-pill .sdot{ width:5px; height:5px; border-radius:50%; background: currentColor; }
  .speeds{ display:flex; gap: 22px; margin-bottom: 12px; }
  .speed-block .lbl{ font-family: var(--font-mono); font-size: 10px; letter-spacing: .5px; text-transform: uppercase; color: var(--text-faint); display:flex; align-items:center; gap: 5px; margin-bottom: 4px; }
  .speed-block.down .lbl{ color: var(--down); }
  .speed-block.up .lbl{ color: var(--up); }
  .speed-block .num{ font-family: var(--font-mono); font-size: 21px; font-weight: 600; letter-spacing: -0.5px; }
  .speed-block .num .u{ font-size: 11px; color: var(--text-dim); font-weight: 400; margin-left: 3px;}
  .spark-wrap{ position: relative; height: 54px; margin: 4px 0 14px; border-radius: 6px; overflow: hidden; background: var(--panel-2); }
  .spark-wrap canvas{ width: 100%; height: 100%; display:block; }
  .meta-row{ display:grid; grid-template-columns: repeat(3, 1fr); gap: 10px; padding-top: 12px; border-top: 1px solid var(--line-soft); }
  .meta-item .k{ font-family: var(--font-mono); font-size: 9.5px; letter-spacing: .5px; text-transform: uppercase; color: var(--text-faint); margin-bottom: 3px; }
  .meta-item .v{ font-family: var(--font-mono); font-size: 12px; color: var(--text-dim); word-break: break-all; }
  .meta-item .v.strong{ color: var(--text); }
  .empty{ font-family: var(--font-mono); color: var(--text-faint); font-size: 13px; padding: 40px 0; text-align:center; }

  .panels{ display:grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 14px; }
  .panel{ background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 18px 20px; }
  .panel h2{ font-size: 13px; font-weight: 600; margin-bottom: 4px; }
  .panel .panel-sub{ font-family: var(--font-mono); font-size: 10.5px; color: var(--text-faint); margin-bottom: 14px; }

  .outage-row{ display:flex; justify-content: space-between; align-items:center; padding: 7px 0; border-bottom: 1px solid var(--line-soft); font-family: var(--font-mono); font-size: 11.5px; }
  .outage-row:last-child{ border-bottom: none; }
  .outage-row .dur{ color: var(--warn); }
  .outage-stats{ display:flex; gap: 18px; margin-bottom: 12px; }
  .outage-stats .st .n{ font-family: var(--font-mono); font-size: 20px; font-weight: 600; }
  .outage-stats .st .l{ font-family: var(--font-mono); font-size: 9.5px; color: var(--text-faint); text-transform: uppercase; letter-spacing: .5px; margin-top: 2px; }

  .hist-chart{ display:flex; align-items:flex-end; gap: 4px; height: 90px; margin-bottom: 10px; }
  .hist-bar{ flex:1; display:flex; flex-direction:column; justify-content:flex-end; align-items:center; gap:2px; height:100%; position:relative; }
  .hist-bar .bar{ width: 100%; border-radius: 3px 3px 0 0; position:relative; overflow:hidden; display:flex; flex-direction:column-reverse; min-height: 2px; }
  .hist-bar .bar .seg-down{ background: var(--down); width:100%; }
  .hist-bar .bar .seg-up{ background: var(--up); width:100%; }
  .hist-bar .lbl{ font-family: var(--font-mono); font-size: 8.5px; color: var(--text-faint); margin-top: 4px; }
  .hist-totals{ display:flex; gap: 18px; padding-top: 10px; border-top: 1px solid var(--line-soft); }
  .hist-totals .t .n{ font-family: var(--font-mono); font-size: 15px; font-weight: 600; }
  .hist-totals .t .l{ font-family: var(--font-mono); font-size: 9.5px; color: var(--text-faint); text-transform: uppercase; margin-top:2px; }

  .conn-row{ display:flex; justify-content: space-between; align-items:flex-start; padding: 8px 0; border-bottom: 1px solid var(--line-soft); gap: 10px; }
  .conn-row:last-child{ border-bottom: none; }
  .conn-row .pname{ font-family: var(--font-mono); font-size: 12px; font-weight: 600; }
  .conn-row .premote{ font-family: var(--font-mono); font-size: 10px; color: var(--text-faint); margin-top: 2px; word-break: break-all; }
  .conn-row .pcount{ font-family: var(--font-mono); font-size: 15px; color: var(--text-dim); flex-shrink:0; }

  .speed-now{ display:flex; gap: 26px; margin-bottom: 12px; }
  .speed-now .sv .n{ font-family: var(--font-mono); font-size: 24px; font-weight: 700; }
  .speed-now .sv.down .n{ color: var(--down); }
  .speed-now .sv.up .n{ color: var(--up); }
  .speed-now .sv .l{ font-family: var(--font-mono); font-size: 9.5px; color: var(--text-faint); text-transform: uppercase; margin-top: 3px; }
  .speed-meta{ font-family: var(--font-mono); font-size: 10.5px; color: var(--text-faint); margin-bottom: 12px; }
  .runbtn{ font-family: var(--font-mono); font-size: 11px; letter-spacing: .5px; text-transform: uppercase; background: var(--panel-2); color: var(--text); border: 1px solid var(--line); border-radius: 6px; padding: 8px 14px; cursor: pointer; }
  .runbtn:hover{ border-color: var(--down); color: var(--down); }
  .runbtn:disabled{ opacity: .5; cursor: default; }
  .speedhist{ display:flex; align-items:flex-end; gap:3px; height:44px; margin-top: 14px; }
  .speedhist .b{ flex:1; background: var(--down); border-radius: 2px 2px 0 0; min-height:2px; }

  footer{ margin-top: 26px; display:flex; justify-content: space-between; flex-wrap: wrap; gap: 10px; font-family: var(--font-mono); font-size: 10.5px; color: var(--text-faint); letter-spacing: .3px; }
  footer span b{ color: var(--text-dim); font-weight: 500; }
  @media (max-width: 640px){ body{ padding: 20px 16px 36px; } .summary{ grid-template-columns: 1fr 1fr; } .speeds{ gap: 14px; } }
</style>
</head>
<body>
  <div class="top">
    <div class="brand">
      <div class="dot" id="brandDot"></div>
      <div>
        <span class="eyebrow">Live adapter telemetry &middot; auto-scanned</span>
        <h1 id="hostTitle">Network Monitor</h1>
      </div>
    </div>
    <div class="clock">
      <div id="time">00:00:00</div>
      <div class="date" id="date">-</div>
    </div>
  </div>

  <div class="summary">
    <div class="cell ring-cell">
      <svg width="58" height="58" viewBox="0 0 58 58">
        <circle cx="29" cy="29" r="24" fill="none" stroke="var(--line)" stroke-width="5"/>
        <circle id="ringDown" cx="29" cy="29" r="24" fill="none" stroke="var(--down)" stroke-width="5" stroke-linecap="round" stroke-dasharray="150.8" stroke-dashoffset="150.8" transform="rotate(-90 29 29)"/>
        <circle id="ringUp" cx="29" cy="29" r="18" fill="none" stroke="var(--up)" stroke-width="4" stroke-linecap="round" stroke-dasharray="113.1" stroke-dashoffset="113.1" transform="rotate(-90 29 29)"/>
      </svg>
      <div class="ring-text">
        <div class="label">Aggregate throughput</div>
        <div class="value" id="aggTotal">0<span class="unit">Mbps</span></div>
      </div>
    </div>
    <div class="cell down">
      <div class="label">Total download</div>
      <div class="value" id="sumDown">0<span class="unit">Mbps</span></div>
    </div>
    <div class="cell up">
      <div class="label">Total upload</div>
      <div class="value" id="sumUp">0<span class="unit">Mbps</span></div>
    </div>
    <div class="cell status">
      <div class="label">Internet status</div>
      <div class="value good" id="netStatus">UP</div>
      <div class="sub" id="pingSub">- ms to 1.1.1.1</div>
    </div>
    <div class="cell">
      <div class="label">Monitor uptime</div>
      <div class="value" id="uptime">00:00:00</div>
      <div class="sub" id="packetsSub">0 packets/s</div>
    </div>
  </div>

  <div class="grid" id="grid"><div class="empty">Scanning adapters...</div></div>

  <div class="panels">
    <div class="panel">
      <h2>Internet health</h2>
      <div class="panel-sub" id="outageSub">Auto-detected from probes to 1.1.1.1, every 3s</div>
      <div class="outage-stats">
        <div class="st"><div class="n" id="outageCount">0</div><div class="l">Outages today</div></div>
        <div class="st"><div class="n" id="outageDowntime">0m</div><div class="l">Downtime today</div></div>
      </div>
      <div id="outageList"><div class="empty" style="padding:16px 0;">No outages logged yet.</div></div>
    </div>

    <div class="panel">
      <h2>Usage history</h2>
      <div class="panel-sub">Persisted across restarts &middot; logged every 60s</div>
      <div class="hist-chart" id="histChart"></div>
      <div class="hist-totals">
        <div class="t"><div class="n" id="histToday">0 GB</div><div class="l">Today</div></div>
        <div class="t"><div class="n" id="histWeek">0 GB</div><div class="l">Last 7 days</div></div>
      </div>
    </div>

    <div class="panel">
      <h2>What's using the network</h2>
      <div class="panel-sub">Active connections grouped by process (not per-app byte totals &mdash; OS sandboxing blocks that without elevated access)</div>
      <div id="connList"><div class="empty" style="padding:16px 0;">Scanning connections...</div></div>
    </div>

    <div class="panel">
      <h2>Speed test</h2>
      <div class="panel-sub">Real measurement against speed.cloudflare.com &middot; auto-runs every 15 min</div>
      <div class="speed-now">
        <div class="sv down"><div class="n" id="stDown">-</div><div class="l">Mbps down</div></div>
        <div class="sv up"><div class="n" id="stUp">-</div><div class="l">Mbps up</div></div>
        <div class="sv"><div class="n" id="stPing">-</div><div class="l">ms ping</div></div>
      </div>
      <div class="speed-meta" id="stMeta">No test run yet.</div>
      <button class="runbtn" id="runBtn">Run test now</button>
      <div class="speedhist" id="speedHist"></div>
    </div>
  </div>

  <footer>
    <span>host <b id="hostname">-</b></span>
    <span>platform <b id="platformName">-</b></span>
    <span>public ip <b id="pubIp">-</b></span>
    <span>db: network_monitor.db</span>
  </footer>

<script>
const KIND_ICON = { wifi:'\\u25CE', ethernet:'\\u25A3', vpn:'\\u25C8', loopback:'\\u25EF', virtual:'\\u25C6', other:'\\u25A1' };
const KIND_LABEL = { wifi:'Wi-Fi (detected)', ethernet:'Ethernet (detected)', vpn:'VPN / tunnel (detected)', loopback:'Loopback', virtual:'Virtual adapter', other:'Network adapter' };

function fmtRate(v){
  if(v < 1) return { n:(v*1000).toFixed(0), u:'Kbps' };
  return { n: v.toFixed(v<10?1:0), u:'Mbps' };
}
function fmtDur(s){
  if(s == null) return '-';
  if(s < 60) return Math.round(s) + 's';
  if(s < 3600) return Math.round(s/60) + 'm';
  return (s/3600).toFixed(1) + 'h';
}
function fmtTime(ts){
  return new Date(ts*1000).toLocaleString('en-US', {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'});
}

function setupCanvas(canvas){
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr; canvas.height = rect.height * dpr;
  const ctx = canvas.getContext('2d'); ctx.scale(dpr, dpr);
  return { ctx, w: rect.width, h: rect.height };
}
function drawSpark(canvas, histDown, histUp, maxVal){
  const { ctx, w, h } = setupCanvas(canvas);
  ctx.clearRect(0,0,w,h);
  const pad = 4; const n = Math.max(histDown.length, 2);
  const drawLine = (hist, color, fillColor)=>{
    const max = Math.max(maxVal, 1); const stepX = w / (n - 1);
    ctx.beginPath();
    hist.forEach((v,i)=>{
      const x = i*stepX; const y = h - pad - (Math.min(v,max)/max) * (h - pad*2);
      if(i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
    });
    ctx.strokeStyle = color; ctx.lineWidth = 1.6; ctx.lineJoin = 'round'; ctx.stroke();
    ctx.lineTo(w, h); ctx.lineTo(0, h); ctx.closePath();
    ctx.fillStyle = fillColor; ctx.fill();
  };
  drawLine(histUp, '#FFB454', 'rgba(255,180,84,0.08)');
  drawLine(histDown, '#4FA8FF', 'rgba(79,168,255,0.12)');
}

const cardEls = {};
const grid = document.getElementById('grid');
function ensureCard(a){
  if(cardEls[a.name]) return cardEls[a.name];
  const card = document.createElement('div');
  card.className = 'card';
  card.innerHTML = `
    <div class="card-head">
      <div class="adapter-name"><div class="icon"></div><div class="txt"><h3></h3><div class="type"></div></div></div>
      <div class="status-pill"><span class="sdot"></span><span class="ptext"></span></div>
    </div>
    <div class="speeds">
      <div class="speed-block down"><div class="lbl"><span>&#8595;</span> download</div><div class="num dn">0<span class="u">Mbps</span></div></div>
      <div class="speed-block up"><div class="lbl"><span>&#8593;</span> upload</div><div class="num up">0<span class="u">Mbps</span></div></div>
    </div>
    <div class="spark-wrap"><canvas></canvas></div>
    <div class="meta-row">
      <div class="meta-item"><div class="k">IPv4</div><div class="v strong m1">-</div></div>
      <div class="meta-item"><div class="k">MAC</div><div class="v m2">-</div></div>
      <div class="meta-item"><div class="k">Link speed</div><div class="v m3">-</div></div>
    </div>`;
  grid.appendChild(card);
  cardEls[a.name] = card;
  return card;
}

function renderStats(data){
  document.getElementById('hostname').textContent = data.hostname;
  document.getElementById('platformName').textContent = data.platform;
  document.getElementById('hostTitle').textContent = data.hostname;
  document.getElementById('pubIp').textContent = data.public_ip || 'unavailable';
  document.getElementById('pingSub').textContent = (data.latency_ms != null ? data.latency_ms + ' ms' : '- ms') + ' to 1.1.1.1';
  document.getElementById('sumDown').innerHTML = data.totals.down_mbps.toFixed(1) + '<span class="unit">Mbps</span>';
  document.getElementById('sumUp').innerHTML = data.totals.up_mbps.toFixed(1) + '<span class="unit">Mbps</span>';
  document.getElementById('aggTotal').innerHTML = (data.totals.down_mbps + data.totals.up_mbps).toFixed(1) + '<span class="unit">Mbps</span>';
  document.getElementById('packetsSub').textContent = Math.round(data.totals.packets_ps).toLocaleString() + ' packets/s';

  const statusEl = document.getElementById('netStatus');
  const dotEl = document.getElementById('brandDot');
  if(data.internet_up){
    statusEl.textContent = 'UP'; statusEl.className = 'value good'; dotEl.className = 'dot';
  } else {
    statusEl.textContent = 'DOWN ' + fmtDur(data.current_outage_seconds); statusEl.className = 'value bad'; dotEl.className = 'dot bad';
  }

  const maxScale = 220;
  document.getElementById('ringDown').style.strokeDashoffset = 150.8 - Math.min(1, data.totals.down_mbps/maxScale)*150.8;
  document.getElementById('ringUp').style.strokeDashoffset = 113.1 - Math.min(1, data.totals.up_mbps/maxScale)*113.1;

  const h = Math.floor(data.uptime_seconds/3600).toString().padStart(2,'0');
  const m = Math.floor((data.uptime_seconds%3600)/60).toString().padStart(2,'0');
  const s = (data.uptime_seconds%60).toString().padStart(2,'0');
  document.getElementById('uptime').textContent = h+':'+m+':'+s;

  if(data.adapters.length === 0){ grid.innerHTML = '<div class="empty">No adapters found.</div>'; return; }
  if(grid.querySelector('.empty')) grid.innerHTML = '';
  const seen = new Set(data.adapters.map(a=>a.name));
  Object.keys(cardEls).forEach(name=>{ if(!seen.has(name)){ cardEls[name].remove(); delete cardEls[name]; } });

  data.adapters.forEach(a=>{
    const card = ensureCard(a);
    card.classList.toggle('offline', !a.isup);
    card.querySelector('.icon').textContent = KIND_ICON[a.kind] || KIND_ICON.other;
    card.querySelector('h3').textContent = a.name;
    card.querySelector('.type').textContent = KIND_LABEL[a.kind] || KIND_LABEL.other;
    const pill = card.querySelector('.status-pill');
    pill.className = 'status-pill ' + (a.isup ? 'up' : 'down');
    pill.querySelector('.ptext').textContent = a.isup ? 'connected' : 'disconnected';
    const fd = fmtRate(a.down_mbps), fu = fmtRate(a.up_mbps);
    card.querySelector('.num.dn').innerHTML = fd.n + '<span class="u">' + fd.u + '</span>';
    card.querySelector('.num.up').innerHTML = fu.n + '<span class="u">' + fu.u + '</span>';
    card.querySelector('.m1').textContent = a.ipv4 || 'none';
    card.querySelector('.m2').textContent = a.mac || '-';
    card.querySelector('.m3').textContent = a.speed_mbps ? (a.speed_mbps + ' Mbps \\u00B7 ' + a.duplex) : 'unknown';
    const maxRef = Math.max(...a.hist_down, ...a.hist_up, 1) * 1.3;
    drawSpark(card.querySelector('canvas'), a.hist_down, a.hist_up, maxRef);
  });
}

function renderOutages(data){
  document.getElementById('outageCount').textContent = data.today_count;
  document.getElementById('outageDowntime').textContent = fmtDur(data.today_total_s);
  const list = document.getElementById('outageList');
  if(data.recent.length === 0){ list.innerHTML = '<div class="empty" style="padding:16px 0;">No outages logged yet.</div>'; return; }
  list.innerHTML = data.recent.map(o => `
    <div class="outage-row">
      <span>${fmtTime(o.start_ts)}</span>
      <span class="dur">${o.duration_s != null ? fmtDur(o.duration_s) : 'ongoing'}</span>
    </div>`).join('');
}

function renderHistory(data){
  document.getElementById('histToday').textContent = (data.today_down_gb + data.today_up_gb).toFixed(2) + ' GB';
  document.getElementById('histWeek').textContent = (data.week_down_gb + data.week_up_gb).toFixed(2) + ' GB';
  const chart = document.getElementById('histChart');
  const days = data.daily.length ? data.daily : [];
  const max = Math.max(...days.map(d=>d.down_gb+d.up_gb), 0.01);
  chart.innerHTML = days.map(d => {
    const total = d.down_gb + d.up_gb;
    const pct = Math.max(2, (total/max)*100);
    const downPct = total > 0 ? (d.down_gb/total)*100 : 50;
    const label = d.day.slice(5).replace('-','/');
    return `<div class="hist-bar" title="${d.day}: ${total.toFixed(2)} GB">
      <div class="bar" style="height:${pct}%">
        <div class="seg-down" style="height:${downPct}%"></div>
        <div class="seg-up" style="height:${100-downPct}%"></div>
      </div>
      <div class="lbl">${label}</div>
    </div>`;
  }).join('') || '<div class="empty">Collecting data...</div>';
}

function renderConnections(data){
  const list = document.getElementById('connList');
  if(!data.processes.length){ list.innerHTML = '<div class="empty" style="padding:16px 0;">No active connections found.</div>'; return; }
  list.innerHTML = data.processes.map(p => `
    <div class="conn-row">
      <div>
        <div class="pname">${p.name}</div>
        <div class="premote">${p.remotes.length ? p.remotes.join(', ') : 'no remote peers'}</div>
      </div>
      <div class="pcount">${p.count}</div>
    </div>`).join('');
}

function renderSpeedtests(data){
  const btn = document.getElementById('runBtn');
  btn.disabled = data.running;
  btn.textContent = data.running ? 'Running...' : 'Run test now';
  const hist = data.history;
  if(hist.length){
    const last = hist[hist.length-1];
    document.getElementById('stDown').textContent = last.download_mbps != null ? last.download_mbps.toFixed(1) : '-';
    document.getElementById('stUp').textContent = last.upload_mbps != null ? last.upload_mbps.toFixed(1) : '-';
    document.getElementById('stPing').textContent = last.ping_ms != null ? last.ping_ms.toFixed(0) : '-';
    document.getElementById('stMeta').textContent = 'Last run: ' + fmtTime(last.ts);
  } else if(data.last_error){
    document.getElementById('stMeta').textContent = data.last_error;
  }
  const maxD = Math.max(...hist.map(h=>h.download_mbps||0), 1);
  document.getElementById('speedHist').innerHTML = hist.map(h => {
    const pct = Math.max(3, ((h.download_mbps||0)/maxD)*100);
    return `<div class="b" style="height:${pct}%" title="${fmtTime(h.ts)}: ${(h.download_mbps||0).toFixed(1)} Mbps"></div>`;
  }).join('');
}

function updateClock(){
  const now = new Date();
  document.getElementById('time').textContent = now.toLocaleTimeString('en-US', {hour12:false});
  document.getElementById('date').textContent = now.toLocaleDateString('en-US', { weekday:'short', month:'short', day:'numeric', year:'numeric' });
}

async function pollStats(){ try{ renderStats(await (await fetch('/api/stats')).json()); }catch(e){} }
async function pollOutages(){ try{ renderOutages(await (await fetch('/api/outages')).json()); }catch(e){} }
async function pollHistory(){ try{ renderHistory(await (await fetch('/api/history')).json()); }catch(e){} }
async function pollConnections(){ try{ renderConnections(await (await fetch('/api/connections')).json()); }catch(e){} }
async function pollSpeedtests(){ try{ renderSpeedtests(await (await fetch('/api/speedtests')).json()); }catch(e){} }

document.getElementById('runBtn').addEventListener('click', async ()=>{
  await fetch('/api/speedtest/run', {method:'POST'});
  pollSpeedtests();
});

updateClock();
pollStats(); pollOutages(); pollHistory(); pollConnections(); pollSpeedtests();
setInterval(updateClock, 1000);
setInterval(pollStats, 1000);
setInterval(pollConnections, 3000);
setInterval(pollOutages, 15000);
setInterval(pollHistory, 30000);
setInterval(pollSpeedtests, 10000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/stats"):
            self._json(build_snapshot())
        elif self.path.startswith("/api/outages"):
            self._json(build_outages())
        elif self.path.startswith("/api/history"):
            self._json(build_history())
        elif self.path.startswith("/api/connections"):
            self._json(build_connections())
        elif self.path.startswith("/api/speedtests"):
            self._json(build_speedtests())
        else:
            body = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def do_POST(self):
        if self.path.startswith("/api/speedtest/run"):
            threading.Thread(target=run_speed_test, daemon=True).start()
            self._json({"started": True})
        else:
            self.send_response(404)
            self.end_headers()


def main():
    port = PORT
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass

    db_init()

    threading.Thread(target=sampler, daemon=True).start()
    threading.Thread(target=latency_probe, daemon=True).start()
    threading.Thread(target=public_ip_probe, daemon=True).start()
    threading.Thread(target=usage_logger, daemon=True).start()
    threading.Thread(target=connections_sampler, daemon=True).start()
    threading.Thread(target=speedtest_scheduler, daemon=True).start()

    time.sleep(1.2)

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    url = "http://127.0.0.1:%d" % port
    print("Network monitor running at %s" % url)
    print("History is saved to %s (survives restarts)." % DB_PATH)
    print("Press Ctrl+C to stop.")
    threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping.")
        server.shutdown()


if __name__ == "__main__":
    main()
