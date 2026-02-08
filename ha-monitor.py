import os
import time
import threading
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request, render_template

APP_NAME = "HA Monitor"
DEFAULT_POLL_INTERVAL_SECONDS = 2
DEFAULT_REMINDER_COOLDOWN_SECONDS = 60

EQ_ABS_TOL = float(os.environ.get("EQ_ABS_TOL", "1e-4"))

DB_PATH = os.environ.get("DB_PATH", "/data/app.db")

app = Flask(__name__)

_cache_lock = threading.Lock()
_state_cache: Dict[str, Any] = {
    "updated_at": None,
    "last_poll_ok_at": None,
    "last_ha_error": None,
    "items": [],
}

_poller_stop = threading.Event()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def log(msg: str) -> None:
    print(f"[{now_iso()}] {msg}", flush=True)


# -----------------------------
# DB helpers + migrations
# -----------------------------
def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _table_has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = [r["name"] for r in cur.fetchall()]
    return col in cols


def _config_set_if_absent(conn: sqlite3.Connection, key: str, value: str) -> None:
    cur = conn.execute("SELECT value FROM config WHERE key = ?", (key,))
    if cur.fetchone() is None:
        conn.execute("INSERT INTO config(key, value) VALUES(?, ?)", (key, value))


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = _db_connect()
    try:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS entities (
            entity_id TEXT PRIMARY KEY
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id TEXT NOT NULL,
            op TEXT NOT NULL,
            threshold REAL NOT NULL,
            text_tpl TEXT NOT NULL,
            desp_tpl TEXT NOT NULL,
            fired INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            fired_at TEXT,
            last_error TEXT
        );
        """)
        conn.commit()

        # entities sort_order
        if not _table_has_column(conn, "entities", "sort_order"):
            conn.execute("ALTER TABLE entities ADD COLUMN sort_order INTEGER;")
            conn.commit()

        # Fill missing sort_order
        cur = conn.execute("SELECT entity_id, sort_order FROM entities ORDER BY entity_id ASC")
        rows = cur.fetchall()
        if any((r["sort_order"] is None) for r in rows):
            i = 1
            for r in rows:
                if r["sort_order"] is None:
                    conn.execute("UPDATE entities SET sort_order=? WHERE entity_id=?", (i, r["entity_id"]))
                    i += 1
            conn.commit()

        # reminders last_attempt_at
        if not _table_has_column(conn, "reminders", "last_attempt_at"):
            conn.execute("ALTER TABLE reminders ADD COLUMN last_attempt_at TEXT;")
            conn.commit()

        # reminders last_fired_at
        if not _table_has_column(conn, "reminders", "last_fired_at"):
            conn.execute("ALTER TABLE reminders ADD COLUMN last_fired_at TEXT;")
            conn.commit()

        # migrate old fired_at -> last_fired_at if needed
        try:
            if _table_has_column(conn, "reminders", "fired_at"):
                conn.execute("""
                    UPDATE reminders
                    SET last_fired_at = COALESCE(NULLIF(fired_at,''), '')
                    WHERE (last_fired_at IS NULL OR last_fired_at = '')
                      AND (fired_at IS NOT NULL AND fired_at <> '')
                """)
                conn.commit()
        except Exception:
            pass

        # defaults
        _config_set_if_absent(conn, "ha_base_url", "")
        _config_set_if_absent(conn, "ha_token", "")
        _config_set_if_absent(conn, "ha_verify_tls", "1")
        _config_set_if_absent(conn, "serverchan_sendkey", "")
        _config_set_if_absent(conn, "poll_interval_seconds", str(DEFAULT_POLL_INTERVAL_SECONDS))
        _config_set_if_absent(conn, "reminder_cooldown_seconds", str(DEFAULT_REMINDER_COOLDOWN_SECONDS))
        _config_set_if_absent(conn, "active_entity_id", "")
        conn.commit()
    finally:
        conn.close()


def config_get(key: str, default: str = "") -> str:
    conn = _db_connect()
    try:
        cur = conn.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = cur.fetchone()
        return str(row["value"]) if row else default
    finally:
        conn.close()


def config_set(key: str, value: str) -> None:
    conn = _db_connect()
    try:
        conn.execute(
            "INSERT INTO config(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def _int_cfg(key: str, default: int) -> int:
    raw = config_get(key, str(default)).strip()
    try:
        return max(1, int(float(raw)))
    except Exception:
        return default


# -----------------------------
# Entities with ordering
# -----------------------------
def entities_list() -> List[str]:
    conn = _db_connect()
    try:
        cur = conn.execute("""
            SELECT entity_id
            FROM entities
            ORDER BY
              CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END,
              sort_order ASC,
              entity_id ASC
        """)
        return [r["entity_id"] for r in cur.fetchall()]
    finally:
        conn.close()


def _entities_next_sort_order(conn: sqlite3.Connection) -> int:
    cur = conn.execute("SELECT COALESCE(MAX(sort_order), 0) AS m FROM entities")
    row = cur.fetchone()
    return int(row["m"] or 0) + 1


def entity_add(entity_id: str) -> None:
    conn = _db_connect()
    try:
        cur = conn.execute("SELECT entity_id FROM entities WHERE entity_id=?", (entity_id,))
        if cur.fetchone() is not None:
            return
        so = _entities_next_sort_order(conn)
        conn.execute("INSERT INTO entities(entity_id, sort_order) VALUES(?, ?)", (entity_id, so))
        conn.commit()
    finally:
        conn.close()


def entity_remove(entity_id: str) -> None:
    conn = _db_connect()
    try:
        conn.execute("DELETE FROM entities WHERE entity_id = ?", (entity_id,))
        conn.execute("DELETE FROM reminders WHERE entity_id = ?", (entity_id,))
        conn.commit()
    finally:
        conn.close()

    if config_get("active_entity_id", "") == entity_id:
        config_set("active_entity_id", "")


def entities_reorder(new_order: List[str]) -> None:
    new_order = [str(x).strip() for x in new_order if str(x).strip()]
    conn = _db_connect()
    try:
        existing = entities_list()
        existing_set = set(existing)

        filtered = [eid for eid in new_order if eid in existing_set]
        tail = [eid for eid in existing if eid not in set(filtered)]
        final = filtered + tail

        i = 1
        for eid in final:
            conn.execute("UPDATE entities SET sort_order=? WHERE entity_id=?", (i, eid))
            i += 1
        conn.commit()
    finally:
        conn.close()


# -----------------------------
# Reminders list (API)
# -----------------------------
def reminders_list() -> List[Dict[str, Any]]:
    conn = _db_connect()
    try:
        cur = conn.execute("""
            SELECT id, entity_id, op, threshold, text_tpl, desp_tpl,
                   created_at, last_error, last_fired_at
            FROM reminders
            ORDER BY id DESC
        """)
        out = []
        for r in cur.fetchall():
            out.append({
                "id": r["id"],
                "entity_id": r["entity_id"],
                "op": r["op"],
                "threshold": r["threshold"],
                "text_tpl": r["text_tpl"],
                "desp_tpl": r["desp_tpl"],
                "created_at": r["created_at"],
                "last_fired_at": (r["last_fired_at"] or ""),
                "last_error": (r["last_error"] or ""),
            })
        return out
    finally:
        conn.close()


def reminder_add(entity_id: str, threshold: float, text_tpl: str, desp_tpl: str) -> None:
    conn = _db_connect()
    try:
        conn.execute("""
            INSERT INTO reminders(entity_id, op, threshold, text_tpl, desp_tpl, created_at,
                                 last_error, last_attempt_at, last_fired_at)
            VALUES(?, '==', ?, ?, ?, ?, '', '', '')
        """, (entity_id, threshold, text_tpl, desp_tpl, now_iso()))
        conn.commit()
    finally:
        conn.close()


def reminder_delete(reminder_id: int) -> None:
    conn = _db_connect()
    try:
        conn.execute("DELETE FROM reminders WHERE id = ?", (reminder_id,))
        conn.commit()
    finally:
        conn.close()


def reminder_reset(reminder_id: int) -> None:
    conn = _db_connect()
    try:
        conn.execute("""
            UPDATE reminders
            SET last_attempt_at = '', last_fired_at = '', last_error = ''
            WHERE id = ?
        """, (reminder_id,))
        conn.commit()
    finally:
        conn.close()


def reminders_reset_all() -> None:
    conn = _db_connect()
    try:
        conn.execute("""
            UPDATE reminders
            SET last_attempt_at = '', last_fired_at = '', last_error = ''
        """)
        conn.commit()
    finally:
        conn.close()


# -----------------------------
# Home Assistant + ServerChan
# -----------------------------
def _ha_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _try_float(v: Any) -> Optional[float]:
    try:
        s = str(v).strip()
        if not s:
            return None
        low = s.lower()
        if low in ("unknown", "unavailable", "none", "null"):
            return None
        if "," in s and "." not in s:
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None


def _compare_eq(val: float, threshold: float) -> bool:
    return abs(val - threshold) <= EQ_ABS_TOL


def serverchan_send(text: str, desp: str, sendkey: str, timeout: float = 10.0) -> Tuple[bool, str]:
    k = (sendkey or "").strip()
    if not k:
        return False, "SendKey is empty"
    url = f"https://sctapi.ftqq.com/{k}.send"
    try:
        resp = requests.post(url, data={"text": text, "desp": desp}, timeout=timeout)
        if 200 <= resp.status_code < 300:
            return True, resp.text
        return False, f"HTTP {resp.status_code}: {resp.text}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _render_template(tpl: str, ctx: Dict[str, Any]) -> str:
    out = tpl
    for k, v in ctx.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def poller_loop() -> None:
    while not _poller_stop.is_set():
        poll_interval = _int_cfg("poll_interval_seconds", DEFAULT_POLL_INTERVAL_SECONDS)
        cooldown = _int_cfg("reminder_cooldown_seconds", DEFAULT_REMINDER_COOLDOWN_SECONDS)

        base_url = config_get("ha_base_url", "").strip().rstrip("/")
        token = config_get("ha_token", "").strip()
        verify_tls = config_get("ha_verify_tls", "1").strip() != "0"
        sendkey = config_get("serverchan_sendkey", "").strip()

        eids = entities_list()
        if not base_url or not token or not eids:
            with _cache_lock:
                _state_cache["updated_at"] = now_iso()
                _state_cache["last_ha_error"] = "Not configured (base_url/token/entities empty)"
                _state_cache["items"] = []
            time.sleep(poll_interval)
            continue

        # 1) fetch HA states
        numeric_map: Dict[str, float] = {}
        try:
            r = requests.get(
                f"{base_url}/api/states",
                headers=_ha_headers(token),
                timeout=10.0,
                verify=verify_tls,
            )
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized (LLAT invalid?)")
            r.raise_for_status()

            states = r.json()
            state_map: Dict[str, Any] = {}
            if isinstance(states, list):
                for obj in states:
                    if isinstance(obj, dict) and "entity_id" in obj:
                        state_map[str(obj["entity_id"])] = obj

            items: List[Dict[str, Any]] = []
            for eid in eids:
                obj = state_map.get(eid)
                if not obj:
                    items.append({
                        "entity_id": eid,
                        "name": "-",
                        "state": "NOT_FOUND",
                        "is_numeric": False,
                        "last_changed": "-",
                        "last_updated": "-"
                    })
                    continue

                attrs = obj.get("attributes") or {}
                name = attrs.get("friendly_name") or "-"
                state = obj.get("state")
                num = _try_float(state)
                is_numeric = num is not None
                if is_numeric:
                    numeric_map[eid] = float(num)

                items.append({
                    "entity_id": eid,
                    "name": str(name),
                    "state": str(state),
                    "is_numeric": is_numeric,
                    "last_changed": str(obj.get("last_changed") or "-"),
                    "last_updated": str(obj.get("last_updated") or "-")
                })

            with _cache_lock:
                _state_cache["updated_at"] = now_iso()
                _state_cache["last_poll_ok_at"] = now_iso()
                _state_cache["last_ha_error"] = ""
                _state_cache["items"] = items

        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            with _cache_lock:
                _state_cache["updated_at"] = now_iso()
                _state_cache["last_ha_error"] = err
            log(f"HA fetch error: {err}")
            time.sleep(poll_interval)
            continue

        # 2) evaluate reminders
        now = now_iso()
        conn = _db_connect()
        try:
            cur = conn.execute("""
                SELECT id, entity_id, threshold, text_tpl, desp_tpl, last_attempt_at
                FROM reminders
                ORDER BY id ASC
            """)
            reminders = cur.fetchall()

            for rr in reminders:
                rid = int(rr["id"])
                eid = str(rr["entity_id"])
                thr = float(rr["threshold"])
                last_attempt_at = str(rr["last_attempt_at"] or "").strip()

                if eid not in numeric_map:
                    continue
                val = float(numeric_map[eid])

                if not _compare_eq(val, thr):
                    continue

                # cooldown
                if last_attempt_at:
                    try:
                        last_dt = datetime.fromisoformat(last_attempt_at)
                        if (datetime.now() - last_dt).total_seconds() < cooldown:
                            continue
                    except Exception:
                        pass

                # update last attempt first
                conn.execute("UPDATE reminders SET last_attempt_at=? WHERE id=?", (now, rid))
                conn.commit()

                ctx = {"entity_id": eid, "value": val, "op": "==", "threshold": thr, "time": now}
                text = _render_template(str(rr["text_tpl"]), ctx)
                desp = _render_template(str(rr["desp_tpl"]), ctx)

                if not sendkey:
                    conn.execute("UPDATE reminders SET last_error=? WHERE id=?",
                                 ("SendKey not set", rid))
                    conn.commit()
                    continue

                ok, resp_text = serverchan_send(text=text, desp=desp, sendkey=sendkey)
                if ok:
                    conn.execute("UPDATE reminders SET last_fired_at=?, last_error='' WHERE id=?",
                                 (now, rid))
                    conn.commit()
                    log(f"Reminder fired OK #{rid}: eid={eid} val={val} thr={thr}")
                else:
                    conn.execute("UPDATE reminders SET last_error=? WHERE id=?",
                                 (resp_text, rid))
                    conn.commit()
                    log(f"Reminder send FAILED #{rid}: {resp_text}")

        finally:
            conn.close()

        time.sleep(poll_interval)


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def index():
    return render_template("index.html", app_name=APP_NAME)


@app.get("/api/config")
def api_get_config():
    return jsonify({
        "ha_base_url": config_get("ha_base_url", ""),
        "ha_token": config_get("ha_token", ""),
        "ha_verify_tls": config_get("ha_verify_tls", "1"),
        "serverchan_sendkey": config_get("serverchan_sendkey", ""),
        "poll_interval_seconds": config_get("poll_interval_seconds", str(DEFAULT_POLL_INTERVAL_SECONDS)),
        "reminder_cooldown_seconds": config_get("reminder_cooldown_seconds", str(DEFAULT_REMINDER_COOLDOWN_SECONDS)),
        "active_entity_id": config_get("active_entity_id", "")
    })


@app.post("/api/config")
def api_set_config():
    data = request.get_json(force=True, silent=True) or {}

    def sget(k: str, d: str = "") -> str:
        return str(data.get(k, d)).strip()

    config_set("ha_base_url", sget("ha_base_url", ""))
    config_set("ha_token", sget("ha_token", ""))
    config_set("ha_verify_tls", "1" if sget("ha_verify_tls", "1") != "0" else "0")
    config_set("serverchan_sendkey", sget("serverchan_sendkey", ""))
    config_set("poll_interval_seconds", sget("poll_interval_seconds", str(DEFAULT_POLL_INTERVAL_SECONDS)) or str(DEFAULT_POLL_INTERVAL_SECONDS))
    config_set("reminder_cooldown_seconds", sget("reminder_cooldown_seconds", str(DEFAULT_REMINDER_COOLDOWN_SECONDS)) or str(DEFAULT_REMINDER_COOLDOWN_SECONDS))
    config_set("active_entity_id", sget("active_entity_id", ""))
    return jsonify({"ok": True})


@app.post("/api/test/ha")
def api_test_ha():
    base_url = config_get("ha_base_url", "").strip().rstrip("/")
    token = config_get("ha_token", "").strip()
    verify_tls = config_get("ha_verify_tls", "1").strip() != "0"
    if not base_url or not token:
        return jsonify({"ok": False, "error": "HA base_url or token is empty"}), 400

    try:
        r = requests.get(f"{base_url}/api/states", headers=_ha_headers(token), timeout=10.0, verify=verify_tls)
        if r.status_code == 401:
            return jsonify({"ok": False, "error": "401 Unauthorized (LLAT invalid?)"}), 401
        r.raise_for_status()
        data = r.json()
        n = len(data) if isinstance(data, list) else 0
        return jsonify({"ok": True, "count": n})
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


@app.post("/api/test/serverchan")
def api_test_serverchan():
    sendkey = config_get("serverchan_sendkey", "").strip()
    data = request.get_json(force=True, silent=True) or {}
    text = str(data.get("text", "ServerChan Test")).strip() or "ServerChan Test"
    desp = str(data.get("desp", "Test from HA Monitor")).strip()
    ok, resp_text = serverchan_send(text=text, desp=desp, sendkey=sendkey)
    if ok:
        return jsonify({"ok": True, "resp": resp_text})
    return jsonify({"ok": False, "error": resp_text}), 500


@app.get("/api/entities")
def api_list_entities():
    return jsonify({"entities": entities_list()})


@app.post("/api/entities")
def api_add_entity():
    data = request.get_json(force=True, silent=True) or {}
    entity_id = str(data.get("entity_id", "")).strip()
    if not entity_id or " " in entity_id:
        return jsonify({"ok": False, "error": "Invalid entity_id"}), 400

    entity_add(entity_id)
    if not config_get("active_entity_id", "").strip():
        config_set("active_entity_id", entity_id)

    return jsonify({"ok": True})


@app.post("/api/entities/reorder")
def api_reorder_entities():
    data = request.get_json(force=True, silent=True) or {}
    order = data.get("order", [])
    if not isinstance(order, list):
        return jsonify({"ok": False, "error": "order must be a list"}), 400
    entities_reorder(order)
    return jsonify({"ok": True})


@app.delete("/api/entities/<path:entity_id>")
def api_delete_entity(entity_id: str):
    entity_remove(entity_id)
    return jsonify({"ok": True})


@app.get("/api/states")
def api_states():
    with _cache_lock:
        return jsonify({
            "updated_at": _state_cache.get("updated_at"),
            "last_poll_ok_at": _state_cache.get("last_poll_ok_at"),
            "last_ha_error": _state_cache.get("last_ha_error"),
            "items": _state_cache.get("items", []),
            "active_entity_id": config_get("active_entity_id", "")
        })


@app.get("/api/reminders")
def api_list_reminders():
    return jsonify({"reminders": reminders_list()})


@app.post("/api/reminders")
def api_add_reminder():
    data = request.get_json(force=True, silent=True) or {}
    entity_id = str(data.get("entity_id", "")).strip()
    threshold_raw = data.get("threshold", None)
    text_tpl = str(data.get("text_tpl", "")).strip()
    desp_tpl = str(data.get("desp_tpl", "")).strip()

    try:
        threshold = float(threshold_raw)
    except Exception:
        return jsonify({"ok": False, "error": "Threshold must be a number"}), 400

    if not entity_id:
        return jsonify({"ok": False, "error": "entity_id required"}), 400
    if not text_tpl:
        return jsonify({"ok": False, "error": "text_tpl required"}), 400

    reminder_add(entity_id, threshold, text_tpl, desp_tpl)
    return jsonify({"ok": True})


@app.delete("/api/reminders/<int:reminder_id>")
def api_delete_reminder(reminder_id: int):
    reminder_delete(reminder_id)
    return jsonify({"ok": True})


@app.post("/api/reminders/<int:reminder_id>/reset")
def api_reset_reminder(reminder_id: int):
    reminder_reset(reminder_id)
    return jsonify({"ok": True})


@app.post("/api/reminders/reset_all")
def api_reset_all_reminders():
    reminders_reset_all()
    return jsonify({"ok": True})


def start_poller():
    t = threading.Thread(target=poller_loop, daemon=True)
    t.start()
    log(f"Poller started. EQ_ABS_TOL={EQ_ABS_TOL}")


if __name__ == "__main__":
    init_db()
    start_poller()
    app.run(host="0.0.0.0", port=8080, threaded=True)
