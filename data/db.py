#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据层：本机 sqlite 文件，重启不丢。只用 Python 标准库。"""
import json
import os
import sqlite3
import threading
import time

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, 'data', 'site.db')

# 在线判定阈值（具名常量，README 有说明）
ONLINE_TIMEOUT_MS = 15000
# 留言内容长度上限（字符数，trim 后计）
MESSAGE_MAX_LEN = 500
# 匿名 id / 房间 id / 会话 id 的入参长度上限
ID_MAX_LEN = 64

_lock = threading.Lock()
_local = threading.local()


def _conn():
    """每线程一个连接，避免 ThreadingHTTPServer 下跨线程复用报错。

    部分 Android 原生 / 沙盒文件系统在「刚删掉 db 又立刻重建」时会偶发
    OperationalError: disk I/O error（与 WAL、锁无关，是文件系统层行为）。
    这里做短重试自愈；真机上库文件长期存在，正常只有一次连接。
    """
    c = getattr(_local, 'conn', None)
    if c is not None:
        return c
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    last = None
    for attempt in range(24):
        try:
            c = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
            c.row_factory = sqlite3.Row
            # 不强制 WAL（WAL 在部分 Android 文件系统直接报 disk I/O error）。
            # 所有写操作由 _lock 串行，单进程内一致性有保证。
            c.execute('PRAGMA journal_mode=DELETE')
            c.execute('PRAGMA synchronous=OFF')
            c.execute('PRAGMA busy_timeout=5000')
            # 立刻做一次真实写入，把「文件建了但写不进去」也纳入重试范围
            c.execute('CREATE TABLE IF NOT EXISTS _boot_probe(x)')
            c.commit()
            _local.conn = c
            return c
        except sqlite3.OperationalError as e:
            last = e
            try:
                c.close()
            except Exception:
                pass
            time.sleep(0.04)
    raise last


def init():
    with _lock:
        c = _conn()
        c.executescript('''
        CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );
        -- F2：累计打开次数（单行）+ 会话去重表
        CREATE TABLE IF NOT EXISTS visit_sessions (
            vid TEXT NOT NULL,
            sid TEXT NOT NULL,
            ts  INTEGER NOT NULL,
            PRIMARY KEY (vid, sid)
        );
        -- F3：在线心跳表
        CREATE TABLE IF NOT EXISTS online (
            vid        TEXT PRIMARY KEY,
            last_seen  INTEGER NOT NULL
        );
        -- F4：留言（content_html 为后端 html.escape 后的安全文本）
        CREATE TABLE IF NOT EXISTS messages (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            content_html TEXT NOT NULL,
            ts           INTEGER NOT NULL
        );
        -- F6：房间与房间事件（房间为实时会话，事件落库便于审计）
        -- game: rps / dice / bubble；state: 该玩法的通用同步状态 JSON
        CREATE TABLE IF NOT EXISTS rooms (
            id      TEXT PRIMARY KEY,
            a       TEXT,
            b       TEXT,
            game    TEXT NOT NULL DEFAULT 'rps',
            state   TEXT NOT NULL DEFAULT '{}',
            move_a  INTEGER,
            move_b  INTEGER,
            round   INTEGER NOT NULL DEFAULT 1,
            updated INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS room_events (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL,
            vid     TEXT,
            action  TEXT NOT NULL,
            payload TEXT,
            ts      INTEGER NOT NULL
        );
        ''')
        # 兼容旧库（若之前建过不含 game/state 的 rooms 表）
        cols = [r['name'] for r in c.execute('PRAGMA table_info(rooms)').fetchall()]
        if 'game' not in cols:
            c.execute("ALTER TABLE rooms ADD COLUMN game TEXT NOT NULL DEFAULT 'rps'")
        if 'state' not in cols:
            c.execute("ALTER TABLE rooms ADD COLUMN state TEXT NOT NULL DEFAULT '{}'")
        c.execute("INSERT OR IGNORE INTO meta(k,v) VALUES('visits_total','0')")
        c.commit()


def now_ms():
    return int(time.time() * 1000)


# ---------- F2 访客记录 ----------
def add_visit(vid, sid):
    """同一 (vid, sid) 只计一次；返回累计总数。"""
    with _lock:
        c = _conn()
        cur = c.execute('INSERT OR IGNORE INTO visit_sessions(vid,sid,ts) VALUES(?,?,?)',
                        (vid, sid, now_ms()))
        if cur.rowcount:
            c.execute("UPDATE meta SET v=CAST(v AS INTEGER)+1 WHERE k='visits_total'")
            c.commit()
        row = c.execute("SELECT v FROM meta WHERE k='visits_total'").fetchone()
        return int(row['v'])


def visit_total():
    with _lock:
        row = _conn().execute("SELECT v FROM meta WHERE k='visits_total'").fetchone()
        return int(row['v'])


# ---------- F3 在线人数 ----------
def heartbeat(vid):
    with _lock:
        c = _conn()
        c.execute('INSERT INTO online(vid,last_seen) VALUES(?,?) '
                  'ON CONFLICT(vid) DO UPDATE SET last_seen=excluded.last_seen',
                  (vid, now_ms()))
        c.commit()


def online_count():
    """先清超时，再数人数。"""
    with _lock:
        c = _conn()
        c.execute('DELETE FROM online WHERE last_seen < ?', (now_ms() - ONLINE_TIMEOUT_MS,))
        c.commit()
        return c.execute('SELECT COUNT(*) AS n FROM online').fetchone()['n']


def is_online(vid):
    with _lock:
        row = _conn().execute('SELECT last_seen FROM online WHERE vid=?', (vid,)).fetchone()
        return bool(row) and row['last_seen'] >= now_ms() - ONLINE_TIMEOUT_MS


def drop_offline(vid):
    with _lock:
        c = _conn()
        c.execute('DELETE FROM online WHERE vid=?', (vid,))
        c.commit()


# ---------- F4 留言板 ----------
def add_message(content_html, ts):
    with _lock:
        c = _conn()
        c.execute('INSERT INTO messages(content_html,ts) VALUES(?,?)', (content_html, ts))
        c.commit()
        return c.execute('SELECT last_insert_rowid() AS id').fetchone()['id']


def list_messages():
    """按时间正序（id 升序）返回全部留言，不截断、不过期。"""
    with _lock:
        rows = _conn().execute('SELECT id,content_html,ts FROM messages ORDER BY id ASC').fetchall()
        return [dict(r) for r in rows]


# ---------- F6 房间 ----------
def create_room(room_id, a, b, game):
    with _lock:
        c = _conn()
        c.execute('INSERT OR REPLACE INTO rooms(id,a,b,game,state,move_a,move_b,round,updated) '
                  'VALUES(?,?,?,?,?,NULL,NULL,1,?)', (room_id, a, b, game, '{}', now_ms()))
        c.commit()


def set_state(room_id, state):
    with _lock:
        c = _conn()
        c.execute('UPDATE rooms SET state=?,updated=? WHERE id=?',
                  (json.dumps(state, ensure_ascii=False), now_ms(), room_id))
        c.commit()


def get_room(room_id):
    with _lock:
        r = _conn().execute('SELECT * FROM rooms WHERE id=?', (room_id,)).fetchone()
        return dict(r) if r else None


def set_move(room_id, seat, move):
    with _lock:
        c = _conn()
        col = 'move_a' if seat == 'a' else 'move_b'
        c.execute('UPDATE rooms SET %s=?, updated=? WHERE id=?' % col, (move, now_ms(), room_id))
        c.commit()


def both_moved(room_id):
    with _lock:
        r = _conn().execute('SELECT move_a,move_b FROM rooms WHERE id=?', (room_id,)).fetchone()
        return bool(r) and r['move_a'] is not None and r['move_b'] is not None


def next_round(room_id):
    with _lock:
        c = _conn()
        c.execute('UPDATE rooms SET move_a=NULL,move_b=NULL,round=round+1,updated=? WHERE id=?',
                  (now_ms(), room_id))
        c.commit()


def log_event(room_id, vid, action, payload):
    with _lock:
        c = _conn()
        c.execute('INSERT INTO room_events(room_id,vid,action,payload,ts) VALUES(?,?,?,?,?)',
                  (room_id, vid, action, payload, now_ms()))
        c.commit()
