#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zeroTermux 原生态单文件后端（Python 3 标准库，零第三方依赖）
- 同源提供静态前端 + /api/* 接口，无跨域问题
- sqlite 落 data/site.db，重启不丢
- SSE 长连接做房间广播，短轮询做兜底（微信 WebView 亦可）
- 视频支持 Range（206），横屏 boot1.mp4 / 竖屏 boot2.mp4 可直接拖动播放
启动：python3 server.py [port]   默认 0.0.0.0:8080
"""
import html
import json
import os
import queue
import re
import sys
import threading
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import db

BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE, 'config')
BANNED_FILE = os.path.join(CONFIG_DIR, 'banned-words.json')
ANNOUNCE_FILE = os.path.join(CONFIG_DIR, 'announcement.md')
ID_MAX_LEN = db.ID_MAX_LEN
MSG_MAX_LEN = db.MESSAGE_MAX_LEN
ONLINE_TIMEOUT_MS = db.ONLINE_TIMEOUT_MS
TZ_CN = timezone(timedelta(hours=8))          # 留言时间固定 UTC+8，避免设备时区差异

# F6 三种联机玩法（主页一行 3 个里的后两个 + 弹框的「匹配联机」）
GAMES = ('rps', 'dice', 'bubble')
BUBBLE_COUNT = 36                             # 6×6 泡泡，下标 0..35

MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.md': 'text/markdown; charset=utf-8',
    '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
    '.gif': 'image/gif', '.ico': 'image/x-icon', '.webp': 'image/webp',
    '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8',
    '.mp4': 'video/mp4', '.webm': 'video/webm', '.mov': 'video/quicktime',
    '.ttf': 'font/ttf', '.woff2': 'font/woff2', '.woff': 'font/woff',
}

# ---------- F6 匹配队列：按玩法分桶，不同玩法绝不配到同一房 ----------
MATCH_LOCK = threading.Lock()
MATCH_QUEUE = {g: [] for g in GAMES}          # game -> [等待中的 pid]
MATCH_RESULT = {}                             # pid -> {'roomId','seat','game'}

# ---------- F6 SSE 订阅表：roomId -> [Queue] ----------
SUB_LOCK = threading.Lock()
ROOM_SUBS = {}


def subscribe(room_id, q):
    with SUB_LOCK:
        ROOM_SUBS.setdefault(room_id, []).append(q)


def unsubscribe(room_id, q):
    with SUB_LOCK:
        lst = ROOM_SUBS.get(room_id, [])
        if q in lst:
            lst.remove(q)
        if not lst:
            ROOM_SUBS.pop(room_id, None)


def broadcast(room_id, obj):
    with SUB_LOCK:
        for q in list(ROOM_SUBS.get(room_id, [])):
            try:
                q.put_nowait(obj)
            except queue.Full:
                pass


def room_payload(room_id):
    r = db.get_room(room_id)
    if not r:
        return None
    try:
        state = json.loads(r.get('state') or '{}')
    except Exception:
        state = {}
    # 只回座位是否有人在，不回 pid —— 不显示身份
    return {
        'type': 'state', 'roomId': room_id, 'game': r['game'],
        'seats': {'a': bool(r['a']), 'b': bool(r['b'])},
        'moves': {'a': r['move_a'], 'b': r['move_b']},
        'state': state,
        'round': r['round'], 'updated': r['updated'],
        'timeoutMs': ONLINE_TIMEOUT_MS,
    }


def fmt_ts(ms):
    return datetime.fromtimestamp(ms / 1000, TZ_CN).strftime('%Y-%m-%d %H:%M')


def valid_id(s):
    return isinstance(s, str) and 0 < len(s) <= ID_MAX_LEN and bool(
        re.fullmatch(r'[A-Za-z0-9_.\-]+', s))


def read_config():
    """每次请求都读盘 —— 改配置文件不用重启服务。
    配置文件万一没拷全也不能让 /config.js 直接 500（否则前端整份脚本崩掉、白卡片）。"""
    try:
        with open(BANNED_FILE, 'r', encoding='utf-8') as f:
            words = json.load(f)
    except Exception:
        words = []
    try:
        with open(ANNOUNCE_FILE, 'r', encoding='utf-8') as f:
            announcement = f.read().strip()
    except Exception:
        announcement = ''
    return words, announcement


def prune_queue():
    """剔除已离线/超时的等待者，避免脏队列占着配对位。"""
    for g in GAMES:
        MATCH_QUEUE[g][:] = [v for v in MATCH_QUEUE[g] if db.is_online(v)]


def remove_from_all_queues(pid):
    for g in GAMES:
        MATCH_QUEUE[g][:] = [v for v in MATCH_QUEUE[g] if v != pid]


class Handler(BaseHTTPRequestHandler):
    server_version = 'IdleSite/1.0'
    protocol_version = 'HTTP/1.1'

    def log_message(self, fmt, *args):
        sys.stderr.write('%s - %s\n' % (self.address_string(), fmt % args))

    # ---------- 公共响应工具 ----------
    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Range')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
        self.send_header('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges')

    def json_out(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def body_json(self):
        try:
            n = int(self.headers.get('Content-Length') or 0)
        except ValueError:
            return {}
        if n <= 0 or n > 64 * 1024:
            return {}
        try:
            d = json.loads(self.rfile.read(n).decode('utf-8'))
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Content-Length', '0')
        self._cors()
        self.end_headers()

    # ---------- 静态资源（含 Range，供 mp4 拖动播放） ----------
    def serve_static(self, rel):
        rel = rel.split('?', 1)[0]
        if rel in ('', '/'):
            rel = '/index.html'
        safe = os.path.normpath(rel.lstrip('/')).replace('\\', '/')
        if safe.startswith('..') or safe.startswith('/'):
            return self.json_out({'error': 'bad path'}, 403)
        path = os.path.join(BASE, safe)
        if not os.path.isfile(path):
            return self.json_out({'error': 'not found'}, 404)
        ext = os.path.splitext(path)[1].lower()
        size = os.path.getsize(path)
        ctype = MIME.get(ext, 'application/octet-stream')

        # Range: bytes=start-end
        rng = self.headers.get('Range')
        start, end, partial = 0, size - 1, False
        if rng:
            m = re.match(r'bytes=(\d*)-(\d*)$', rng.strip())
            if m:
                if m.group(1):
                    start = int(m.group(1))
                    if m.group(2):
                        end = min(int(m.group(2)), size - 1)
                elif m.group(2):
                    start = max(0, size - int(m.group(2)))
                if start >= size:
                    self.send_response(416)
                    self.send_header('Content-Range', 'bytes */%d' % size)
                    self.send_header('Content-Length', '0')
                    self._cors()
                    self.end_headers()
                    return
                partial = True

        length = end - start + 1
        with open(path, 'rb') as f:
            f.seek(start)
            body = f.read(length)
        self.send_response(206 if partial else 200)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Accept-Ranges', 'bytes')
        if partial:
            self.send_header('Content-Range', 'bytes %d-%d/%d' % (start, end, size))
        # html/js/css 不缓存：覆盖文件后刷新即生效（原静态服务的 304 问题）
        self.send_header('Cache-Control', 'no-store' if ext in ('.html', '.js', '.css')
                         else 'public, max-age=300')
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def serve_config_js(self):
        """把两个配置文件同步注入前端，保证违禁词检测时序与原版一致。"""
        words, announcement = read_config()
        payload = json.dumps({'bannedWords': words, 'announcement': announcement},
                             ensure_ascii=False).replace('<', '\\u003c')
        body = ('window.__CONFIG__=' + payload + ';\n').encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/javascript; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    # ---------- GET ----------
    def do_GET(self):
        u = urlparse(self.path)
        p, qs = u.path, parse_qs(u.query)

        if p == '/config.js':
            return self.serve_config_js()

        if p == '/api/config':
            w, a = read_config()
            return self.json_out({'bannedWords': w, 'announcement': a,
                                  'games': list(GAMES),
                                  'onlineTimeoutMs': ONLINE_TIMEOUT_MS,
                                  'messageMaxLen': MSG_MAX_LEN})

        # F2
        if p == '/api/visit/stats':
            return self.json_out({'total': db.visit_total()})

        # F3
        if p == '/api/online':
            return self.json_out({'count': db.online_count(),
                                  'timeoutMs': ONLINE_TIMEOUT_MS})

        # F4
        if p == '/api/messages':
            rows = db.list_messages()
            return self.json_out({'messages': [
                {'id': r['id'], 'contentHtml': r['content_html'],
                 'time': fmt_ts(r['ts'])} for r in rows
            ]})

        # F5
        if p == '/api/announcement':
            _, a = read_config()
            return self.json_out({'text': a})

        # F6 匹配状态
        if p == '/api/match/status':
            pid = (qs.get('pid') or [''])[0]
            if not valid_id(pid):
                return self.json_out({'error': 'bad pid'}, 400)
            with MATCH_LOCK:
                prune_queue()
                r = MATCH_RESULT.get(pid)
                return self.json_out(dict(r, status='matched') if r
                                    else {'status': 'waiting'})

        # F6 房间状态（SSE 不可用时的短轮询兜底）
        if p == '/api/room/state':
            rid = (qs.get('roomId') or [''])[0]
            if not valid_id(rid):
                return self.json_out({'error': 'bad roomId'}, 400)
            pl = room_payload(rid)
            return self.json_out(pl or {'error': 'no room'}, 404)

        # F6 SSE 广播流
        if p == '/api/room/stream':
            return self.serve_sse(qs)

        return self.serve_static(p)

    # ---------- SSE ----------
    def serve_sse(self, qs):
        rid = (qs.get('roomId') or [''])[0]
        pid = (qs.get('pid') or [''])[0]
        if not valid_id(rid) or not valid_id(pid):
            return self.json_out({'error': 'bad params'}, 400)
        room = db.get_room(rid)
        if not room or pid not in (room['a'], room['b']):
            return self.json_out({'error': 'not in room'}, 403)

        q = queue.Queue(maxsize=64)
        subscribe(rid, q)
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream; charset=utf-8')
        self.send_header('Cache-Control', 'no-cache, no-transform')
        self.send_header('Connection', 'keep-alive')
        self.send_header('X-Accel-Buffering', 'no')   # 穿透/反代不缓冲
        self._cors()
        self.end_headers()
        try:
            self._sse_write(room_payload(rid))
            while True:
                try:
                    item = q.get(timeout=12)
                    self._sse_write(item)
                except queue.Empty:
                    self.wfile.write(b': keepalive\n\n')
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            unsubscribe(rid, q)

    def _sse_write(self, obj):
        data = json.dumps(obj, ensure_ascii=False).replace('\n', ' ')
        self.wfile.write(('data: ' + data + '\n\n').encode('utf-8'))
        self.wfile.flush()

    # ---------- F6 房间动作校验与状态推进 ----------
    def apply_action(self, room, seat, action, payload, room_id):
        """返回 (new_state, is_reset)；校验不通过抛 ValueError。"""
        state = {}
        try:
            state = json.loads(room.get('state') or '{}')
        except Exception:
            state = {}

        if action == 'reset':
            db.next_round(room_id)
            return {}, True

        if action == 'move':                       # 石头剪刀布（原 F6）
            if not isinstance(payload, int) or payload < 0 or payload > 2:
                raise ValueError('move must be 0..2')
            db.set_move(room_id, seat, payload)
            return state, False

        if action == 'roll':                       # 联机掷骰子
            if not isinstance(payload, dict):
                raise ValueError('bad roll payload')
            v = payload.get('v')
            if not isinstance(v, int) or isinstance(v, bool) or v < 1 or v > 6:
                raise ValueError('roll v must be 1..6')
            rolls = state.setdefault('rolls', {})
            if seat in rolls:
                raise ValueError('already rolled this round')
            rolls[seat] = v
            return state, False

        if action == 'pop':                        # 联机抢泡泡
            if not isinstance(payload, dict):
                raise ValueError('bad pop payload')
            i = payload.get('i')
            if not isinstance(i, int) or isinstance(i, bool) or i < 0 or i >= BUBBLE_COUNT:
                raise ValueError('pop i must be 0..%d' % (BUBBLE_COUNT - 1))
            popped = state.setdefault('popped', [])
            if i in popped:
                raise ValueError('already popped')     # 不重复计分
            popped.append(i)
            score = state.setdefault('score', {'a': 0, 'b': 0})
            score[seat] = score.get(seat, 0) + 1
            return state, False

        raise ValueError('unknown action')

    # ---------- POST ----------
    def do_POST(self):
        p = urlparse(self.path).path
        d = self.body_json()

        # F2：每次打开上报；(vid,sid) 唯一，刷新不暴增
        if p == '/api/visit':
            vid, sid = d.get('vid'), d.get('sid')
            if not valid_id(vid) or not valid_id(sid):
                return self.json_out({'error': 'bad vid/sid'}, 400)
            db.heartbeat(sid)   # 用会话级端 id 记在线，每个标签页算一个端
            return self.json_out({'total': db.add_visit(vid, sid)})

        # F3：心跳
        if p == '/api/online/heartbeat':
            pid = d.get('pid')
            if not valid_id(pid):
                return self.json_out({'error': 'bad pid'}, 400)
            db.heartbeat(pid)
            return self.json_out({'count': db.online_count()})

        # F3：主动离线（页面卸载）
        if p == '/api/online/leave':
            pid = d.get('pid')
            if not valid_id(pid):
                return self.json_out({'error': 'bad pid'}, 400)
            db.drop_offline(pid)
            with MATCH_LOCK:
                remove_from_all_queues(pid)
            return self.json_out({'ok': True})

        # F4：留言（长度校验 + 后端 html.escape 输出转义）
        if p == '/api/messages':
            content = d.get('content')
            if not isinstance(content, str):
                return self.json_out({'error': 'content must be string'}, 400)
            text = content.strip()
            if not text or len(text) > MSG_MAX_LEN:
                return self.json_out({'error': 'empty or too long (max %d)' % MSG_MAX_LEN}, 400)
            safe = html.escape(text, quote=True)
            ts = db.now_ms()
            mid = db.add_message(safe, ts)
            return self.json_out({'ok': True, 'id': mid, 'time': fmt_ts(ts)})

        # F6：加入匹配队列（按 game 分桶）
        if p == '/api/match':
            pid, game = d.get('pid'), d.get('game', 'rps')
            if not valid_id(pid) or game not in GAMES:
                return self.json_out({'error': 'bad pid/game'}, 400)
            db.heartbeat(pid)
            with MATCH_LOCK:
                prune_queue()
                if pid in MATCH_RESULT:
                    return self.json_out(dict(MATCH_RESULT[pid], status='matched'))
                # 一个端同时只排一个玩法：切玩法时先从所有桶移除
                remove_from_all_queues(pid)
                q = MATCH_QUEUE[game]
                if q:
                    other = q.pop(0)
                    rid = uuid.uuid4().hex[:10]
                    db.create_room(rid, other, pid, game)
                    MATCH_RESULT[other] = {'roomId': rid, 'seat': 'a', 'game': game}
                    MATCH_RESULT[pid] = {'roomId': rid, 'seat': 'b', 'game': game}
                    result = dict(MATCH_RESULT[pid], status='matched')
                else:
                    q.append(pid)
                    result = {'status': 'waiting', 'game': game}
            if result.get('status') == 'matched':
                broadcast(result['roomId'], {'type': 'matched', 'roomId': result['roomId']})
                broadcast(result['roomId'], room_payload(result['roomId']))
            return self.json_out(result)

        # F6：退出匹配 / 退出房间
        if p == '/api/match/cancel':
            pid = d.get('pid')
            if not valid_id(pid):
                return self.json_out({'error': 'bad pid'}, 400)
            with MATCH_LOCK:
                remove_from_all_queues(pid)
                MATCH_RESULT.pop(pid, None)
            return self.json_out({'ok': True})

        # F6：房间内动作，后端广播到同房（通用状态同步通道）
        if p == '/api/room/action':
            rid, pid, action, payload = (d.get('roomId'), d.get('pid'),
                                         d.get('action'), d.get('payload'))
            if not valid_id(rid) or not valid_id(pid) or action not in ('move', 'roll', 'pop', 'reset'):
                return self.json_out({'error': 'bad params'}, 400)
            room = db.get_room(rid)
            if not room:
                return self.json_out({'error': 'no room'}, 404)
            seat = 'a' if pid == room['a'] else ('b' if pid == room['b'] else None)
            if seat is None:
                return self.json_out({'error': 'not in room'}, 403)   # 房间隔离
            # 玩法与动作必须匹配，防止跨玩法乱发
            allowed = {'rps': ('move', 'reset'), 'dice': ('roll', 'reset'),
                       'bubble': ('pop', 'reset')}
            if action not in allowed.get(room['game'], ()):
                return self.json_out({'error': 'action not allowed in this game'}, 400)
            try:
                state, _ = self.apply_action(room, seat, action, payload, rid)
            except ValueError as e:
                return self.json_out({'error': str(e)}, 400)
            db.set_state(rid, state)
            db.log_event(rid, pid, action, json.dumps(payload, ensure_ascii=False))
            pl = room_payload(rid)
            broadcast(rid, pl)
            return self.json_out(pl)

        return self.json_out({'error': 'not found'}, 404)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    db.init()
    srv = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    srv.daemon_threads = True
    print('IdleSite backend on 0.0.0.0:%d  db=%s' % (port, db.DB_PATH), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()


if __name__ == '__main__':
    main()
