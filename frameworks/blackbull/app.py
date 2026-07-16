"""BlackBull entrypoint for HttpArena benchmark profiles.

Implements the endpoint contract documented at
https://www.http-arena.com/docs/add-framework/ for the H1, H2,
WebSocket, and gRPC profiles BlackBull supports:

  GET  /pipeline                              → text/plain "ok"
  GET  /baseline11?<int=int>&…                → text/plain sum of query ints
  POST /baseline11?<int=int>&…  body=<int>    → text/plain sum
  GET  /json/{count}?m=<float>                → JSON {items, count}
  GET  /json-comp/{count}?m=<float>           → JSON, may be gzipped
  POST /upload          body                  → text/plain byte count
  GET  /ws (Upgrade)                          → echoes frames
  gRPC GetSum(a,b)                            → SumReply{result=a+b}

Dataset is read from $DATASET_PATH (default /data/dataset.json — the
read-only mount HttpArena's harness provides).

Profiles intentionally NOT implemented:
  - crud               (requires Redis cache integration)
  - *-h3               (no HTTP/3 transport)
  - *-h2c              (h2c-only port validation requires core changes)
  - stream-*grpc       (BlackBull gRPC is unary-only)
  - production-stack / gateway / fortunes

The container starts four BlackBull processes via ``launcher.py``:
cleartext on :8080, h2c on :8082, TLS HTTP/1.1 on :8081, TLS HTTP/2
on :8443.  Cleartext also serves h2c via prior-knowledge — BlackBull
negotiates HTTP/2 on first preface bytes.  gRPC multiplexes onto the
same HTTP/2 ports (cleartext :8080, TLS :8443).
"""
import argparse
import json
import os
import sys
from http import HTTPMethod
from urllib.parse import parse_qs

# Scheme.websocket is the BlackBull marker used by `@app.route` to
# register the `echo-ws` HttpArena profile handler.
from blackbull.utils import Scheme

# Ensure the BlackBull source tree is importable when the Docker image
# vendors it at /src/BlackBull/.  Local runs use `pip install -e .` so
# this is a no-op then.
_repo_root = os.environ.get('BLACKBULL_SRC', '/src/BlackBull')
if os.path.isdir(_repo_root) and _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from blackbull import BlackBull, JSONResponse, Response, read_body
from blackbull.grpc import GrpcServiceRegistry, GrpcStatus
from blackbull.middleware.compression import Compression

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore[assignment]

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

DATASET_PATH = os.environ.get('DATASET_PATH', '/data/dataset.json')
try:
    with open(DATASET_PATH, 'r') as f:
        DATASET_ITEMS = json.load(f)
except (OSError, ValueError):
    DATASET_ITEMS = []


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = BlackBull()

# HttpArena's json-comp profile expects Accept-Encoding-driven compression.
# BlackBull's Compression middleware picks br > zstd > gzip from the codecs
# the container has installed.  Bodies below min_size (default 100 bytes)
# pass through, so /baseline11 + /pipeline aren't affected.
#
# Diagnostic toggle: BB_NO_COMPRESSION=1 skips registering Compression
# entirely.  Useful for isolating the cost of on-the-fly brotli encoding
# on already-compressed payloads (e.g. .woff2 fonts) that lack a
# precompressed sibling.  Not for benchmark publication — disabling a
# default-on feature breaks the apples-to-apples convention.
if os.environ.get('BB_NO_COMPRESSION', '0') != '1':
    app.use(Compression())

# HttpArena's static profile expects /static/<asset> to serve files from
# /data/static/.  app.static() registers a StaticFiles middleware with the
# URL prefix and source directory; missing files (e.g. when /data/static/
# is unpopulated in a sandbox run) return 404 without breaking other routes.
app.static('/static', os.environ.get('STATIC_DIR', '/data/static/'))

_PIPELINE_BODY = b'ok'
_NO_DATASET = b'No dataset'
_PLAIN = 'text/plain; charset=utf-8'


def _qs(scope):
    raw = scope.get('query_string') or b''
    return parse_qs(raw.decode('latin-1'), keep_blank_values=True)


@app.route(path='/pipeline', methods=[HTTPMethod.GET])
async def pipeline():
    return Response(_PIPELINE_BODY, content_type=_PLAIN)


async def _baseline_handler(scope, receive, send):
    """Shared body for /baseline11 (H/1.1) and /baseline2 (H/2).

    HttpArena uses path-suffix to distinguish the two profiles, but
    the semantics are identical: sum integer query params, add posted
    body if integer, return as text/plain.
    """
    total = 0
    for vals in _qs(scope).values():
        for v in vals:
            try:
                total += int(v)
            except ValueError:
                pass
    if scope['method'] == 'POST':
        body = await read_body(receive)
        if body:
            try:
                total += int(body.strip())
            except ValueError:
                pass
    payload = str(total).encode()
    await send({'type': 'http.response.start', 'status': 200,
                'headers': [(b'content-type', _PLAIN.encode())]})
    await send({'type': 'http.response.body', 'body': payload})


@app.route(path='/baseline11', methods=[HTTPMethod.GET, HTTPMethod.POST])
async def baseline11(scope, receive, send):
    await _baseline_handler(scope, receive, send)


# HttpArena's H/2 baseline profile uses /baseline2 (path suffix
# disambiguates from the H/1.1 /baseline11).  Same semantics.
@app.route(path='/baseline2', methods=[HTTPMethod.GET, HTTPMethod.POST])
async def baseline2(scope, receive, send):
    await _baseline_handler(scope, receive, send)


def _json_payload(count: int, m: float):
    items = []
    for idx, ds in enumerate(DATASET_ITEMS):
        if idx >= count:
            break
        item = dict(ds)
        item['total'] = ds['price'] * ds['quantity'] * m
        items.append(item)
    return {'items': items, 'count': len(items)}


@app.route(path='/json/{count:int}', methods=[HTTPMethod.GET])
async def json_endpoint(count: int, scope):
    if not DATASET_ITEMS:
        return Response(_NO_DATASET, status=500, content_type=_PLAIN)
    try:
        m = float(_qs(scope).get('m', ['0'])[0])
    except ValueError:
        m = 0.0
    return JSONResponse(_json_payload(count, m))


@app.route(path='/json-comp/{count:int}', methods=[HTTPMethod.GET])
async def json_comp_endpoint(count: int, scope):
    # Same payload as /json; the Compression middleware registered
    # at module top wraps the response with gzip / brotli / zstd per
    # the client's Accept-Encoding.
    if not DATASET_ITEMS:
        return Response(_NO_DATASET, status=500, content_type=_PLAIN)
    try:
        m = float(_qs(scope).get('m', ['0'])[0])
    except ValueError:
        m = 0.0
    return JSONResponse(_json_payload(count, m))


@app.route(path='/upload', methods=[HTTPMethod.POST])
async def upload_endpoint(scope, receive, send):
    size = 0
    while True:
        msg = await receive()
        if msg['type'] != 'http.request':
            break
        size += len(msg.get('body') or b'')
        if not msg.get('more_body', False):
            break
    payload = str(size).encode()
    await send({'type': 'http.response.start', 'status': 200,
                'headers': [(b'content-type', _PLAIN.encode())]})
    await send({'type': 'http.response.body', 'body': payload})


# Liveness for ``launcher.py``'s readiness probe.
@app.route(path='/healthz', methods=[HTTPMethod.GET])
async def healthz():
    return Response(b'ok', content_type=_PLAIN)


# ---------------------------------------------------------------------------
# gRPC — unary GetSum (benchmark.BenchmarkService/GetSum)
# ---------------------------------------------------------------------------
# HttpArena's h2load sends pre-serialised protobuf for SumRequest{a,b}
# without a protobuf library.  We decode/encode the two-int32 message
# by hand (field 1 = a, field 2 = b; reply field 1 = result).

def _proto_decode_sum_request(data: bytes) -> tuple[int, int]:
    """Decode SumRequest { int32 a = 1; int32 b = 2; } from wire bytes."""
    a = b = 0
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x07
        if wire_type != 0:  # varint only
            raise ValueError(f'Unexpected wire type {wire_type}')
        value, pos = _read_varint(data, pos)
        # protobuf int32 is zigzag-encoded for sint32, but plain int32 uses
        # straight varint (negative values are 10-byte sign-extended).
        # Decode as signed 32-bit.
        if value & (1 << 31):
            value = value - (1 << 32)
        if field_num == 1:
            a = value
        elif field_num == 2:
            b = value
    return a, b


def _proto_encode_sum_reply(result: int) -> bytes:
    """Encode SumReply { int32 result = 1; } to wire bytes."""
    # Field 1, wire type 0 (varint)
    tag = (1 << 3) | 0
    return _encode_varint(tag) + _encode_varint(result & 0xFFFFFFFF)


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    """Read a varint from *data* at *pos*; return (value, new_pos)."""
    value = 0
    shift = 0
    while pos < len(data):
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return value, pos
        shift += 7
    raise ValueError('Truncated varint')


def _encode_varint(value: int) -> bytes:
    """Encode an unsigned integer as a varint."""
    parts = []
    while value > 0x7F:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    parts.append(value & 0x7F)
    return bytes(parts)


grpc_registry = GrpcServiceRegistry()


@grpc_registry.method('/benchmark.BenchmarkService/GetSum')
async def _grpc_get_sum(request: bytes, context) -> bytes:
    a, b = _proto_decode_sum_request(request)
    return _proto_encode_sum_reply(a + b)


app.enable_grpc(grpc_registry)

# ---------------------------------------------------------------------------
# async-db — PostgreSQL via asyncpg
# ---------------------------------------------------------------------------

def _row_to_item(row) -> dict:
    """Convert an asyncpg.Record to the HttpArena item shape."""
    return {
        'id': row['id'], 'name': row['name'], 'category': row['category'],
        'price': row['price'], 'quantity': row['quantity'],
        'active': bool(row['active']),
        'tags': row['tags'] if isinstance(row['tags'], list) else [],
        'rating': {'score': row['rating_score'], 'count': row['rating_count']},
    }


_PG_POOL: 'asyncpg.Pool | None' = None

# HttpArena uses postgres:// scheme; asyncpg requires postgresql://
_DB_URL_RAW = os.environ.get('DATABASE_URL', '')
if _DB_URL_RAW.startswith('postgres://'):
    _DB_URL_RAW = 'postgresql://' + _DB_URL_RAW[len('postgres://'):]

_PG_QUERY = (
    'SELECT id, name, category, price, quantity, active, tags, '
    'rating_score, rating_count '
    'FROM items WHERE price BETWEEN $1 AND $2 LIMIT $3'
)


class _NoResetConnection(asyncpg.Connection):
    """Skip DISCARD ALL on pool return — critical perf optimisation."""
    def get_reset_query(self) -> str:
        return ''


_WORKER_COUNT: int = 1  # Set from --workers arg before app.run()


async def _pg_pool_init() -> 'asyncpg.Pool | None':
    """Initialise the asyncpg pool (lazy — called on first /async-db request)."""
    if asyncpg is None or not _DB_URL_RAW:
        return None
    try:
        _db_max_conn_raw = os.environ.get('DATABASE_MAX_CONN', '')
        if _db_max_conn_raw:
            db_max_conn = int(_db_max_conn_raw)
            pool_size = max(int(db_max_conn * 0.92 / _WORKER_COUNT + 0.95), 2)
        else:
            pool_size = 2  # conservative default (matches FastAPI)
        return await asyncpg.create_pool(
            dsn=_DB_URL_RAW,
            min_size=1,
            max_size=pool_size,
            connection_class=_NoResetConnection,
        )
    except Exception:
        return None


_PG_LOCK = None  # asyncio.Lock, created lazily on first use


async def _get_pool() -> 'asyncpg.Pool | None':
    """Return the shared pool, creating it on first call (race-free)."""
    global _PG_POOL, _PG_LOCK
    if _PG_POOL is not None:
        return _PG_POOL
    if asyncpg is None or not _DB_URL_RAW:
        return None
    import asyncio as _asyncio
    if _PG_LOCK is None:
        _PG_LOCK = _asyncio.Lock()
    async with _PG_LOCK:
        if _PG_POOL is not None:  # double-check under lock
            return _PG_POOL
        _PG_POOL = await _pg_pool_init()
        return _PG_POOL


@app.route(path='/async-db', methods=[HTTPMethod.GET])
async def async_db_endpoint(scope):
    pool = await _get_pool()
    if pool is None:
        return JSONResponse({'items': [], 'count': 0})
    qs = _qs(scope)
    try:
        min_price = int(qs.get('min', ['10'])[0])
        max_price = int(qs.get('max', ['50'])[0])
        limit = int(qs.get('limit', ['50'])[0])
    except (ValueError, IndexError):
        min_price, max_price, limit = 10, 50, 50
    limit = max(1, min(limit, 50))
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(_PG_QUERY, min_price, max_price, limit)
    except Exception:
        return JSONResponse({'items': [], 'count': 0})
    items = [_row_to_item(row) for row in rows]
    return JSONResponse({'items': items, 'count': len(items)})


# ---------------------------------------------------------------------------
# CRUD — PostgreSQL + Redis cache-aside
# ---------------------------------------------------------------------------

_REDIS: 'aioredis.Redis | None' = None
_REDIS_URL = os.environ.get('REDIS_URL', '')
_CRUD_TTL_MS = 200  # matches HttpArena reference implementations

_CRUD_LIST_QUERY = (
    'SELECT id, name, category, price, quantity, active, tags, '
    'rating_score, rating_count '
    'FROM items WHERE category = $1 ORDER BY id LIMIT $2 OFFSET $3'
)
_CRUD_GET_QUERY = (
    'SELECT id, name, category, price, quantity, active, tags, '
    'rating_score, rating_count FROM items WHERE id = $1'
)
_CRUD_CREATE_QUERY = (
    'INSERT INTO items (id, name, category, price, quantity, active, tags, '
    'rating_score, rating_count) '
    'VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) '
    'ON CONFLICT (id) DO UPDATE SET name=$2, category=$3, price=$4, '
    'quantity=$5, active=$6, tags=$7, rating_score=$8, rating_count=$9 '
    'RETURNING id'
)
_CRUD_UPDATE_QUERY = (
    'UPDATE items SET name=$1, category=$2, price=$3, quantity=$4, '
    'active=$5, tags=$6, rating_score=$7, rating_count=$8 '
    'WHERE id=$9'
)


def _crud_cache_key(item_id: int) -> str:
    return f'crud:{item_id}'


async def _crud_cache_get(item_id: int) -> dict | None:
    if _REDIS is None:
        return None
    try:
        raw = await _REDIS.get(_crud_cache_key(item_id))
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return None


async def _crud_cache_set(item_id: int, data: dict) -> None:
    if _REDIS is None:
        return
    try:
        await _REDIS.set(_crud_cache_key(item_id), json.dumps(data), px=_CRUD_TTL_MS)
    except Exception:
        pass


async def _crud_cache_delete(item_id: int) -> None:
    if _REDIS is None:
        return
    try:
        await _REDIS.delete(_crud_cache_key(item_id))
    except Exception:
        pass


def _crud_cache_header(cache_hit: bool) -> dict:
    return {'X-Cache': 'HIT' if cache_hit else 'MISS'}


@app.route(path='/crud/items', methods=[HTTPMethod.GET])
async def crud_list(scope):
    pool = await _get_pool()
    if pool is None:
        return JSONResponse({'items': [], 'total': 0, 'page': 1})
    qs = _qs(scope)
    category = qs.get('category', [''])[0]
    try:
        page = max(1, int(qs.get('page', ['1'])[0]))
        limit = max(1, min(int(qs.get('limit', ['10'])[0]), 100))
    except (ValueError, IndexError):
        page, limit = 1, 10
    offset = (page - 1) * limit
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(_CRUD_LIST_QUERY, category, limit, offset)
    except Exception:
        return JSONResponse({'items': [], 'total': 0, 'page': page})
    items = [_row_to_item(row) for row in rows]
    return JSONResponse({'items': items, 'total': len(items), 'page': page})


@app.route(path='/crud/items', methods=[HTTPMethod.POST])
async def crud_create(body: bytes, scope):
    pool = await _get_pool()
    if pool is None:
        return JSONResponse({'error': 'no database'}, status=500)
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, Exception):
        return JSONResponse({'error': 'invalid json'}, status=400)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                _CRUD_CREATE_QUERY,
                data['id'], data['name'], data['category'],
                data['price'], data['quantity'], data.get('active', True),
                json.dumps(data.get('tags', [])),
                data.get('rating_score', 0), data.get('rating_count', 0),
            )
    except Exception:
        return JSONResponse({'error': 'create failed'}, status=500)
    return JSONResponse(data, status=201)


@app.route(path='/crud/items/{item_id:int}', methods=[HTTPMethod.GET])
async def crud_get(item_id: int, scope):
    # Check cache first
    cached = await _crud_cache_get(item_id)
    if cached is not None:
        return JSONResponse(cached, headers=_crud_cache_header(True))
    pool = await _get_pool()
    if pool is None:
        return JSONResponse({'error': 'not found'}, status=404)
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(_CRUD_GET_QUERY, item_id)
    except Exception:
        return JSONResponse({'error': 'not found'}, status=404)
    if row is None:
        return JSONResponse({'error': 'not found'}, status=404)
    item = _row_to_item(row)
    await _crud_cache_set(item_id, item)
    return JSONResponse(item, headers=_crud_cache_header(False))


@app.route(path='/crud/items/{item_id:int}', methods=[HTTPMethod.PUT])
async def crud_update(item_id: int, body: bytes, scope):
    pool = await _get_pool()
    if pool is None:
        return JSONResponse({'error': 'no database'}, status=500)
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, Exception):
        return JSONResponse({'error': 'invalid json'}, status=400)
    try:
        async with pool.acquire() as conn:
            result = await conn.execute(
                _CRUD_UPDATE_QUERY,
                data['name'], data['category'], data['price'], data['quantity'],
                data.get('active', True), json.dumps(data.get('tags', [])),
                data.get('rating_score', 0), data.get('rating_count', 0),
                item_id,
            )
            # asyncpg execute returns "UPDATE N" — check if any row was updated
            if result == 'UPDATE 0':
                return JSONResponse({'error': 'not found'}, status=404)
    except Exception:
        return JSONResponse({'error': 'update failed'}, status=500)
    # Invalidate cache
    await _crud_cache_delete(item_id)
    data['id'] = item_id
    return JSONResponse(data)


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

# HttpArena `echo-ws` profile — RFC 6455 WebSocket echo.  First
# message after accept is the receive loop; text frames echo as text,
# binary frames echo as bytes.
@app.route(path='/ws', methods=[HTTPMethod.GET], scheme=Scheme.websocket)
async def ws_echo(scope, receive, send):
    event = await receive()
    if event.get('type') != 'websocket.connect':
        return
    await send({'type': 'websocket.accept'})
    while True:
        event = await receive()
        t = event.get('type', '')
        if t == 'websocket.disconnect':
            break
        if t != 'websocket.receive':
            continue
        text = event.get('text')
        if text is not None:
            await send({'type': 'websocket.send', 'text': text})
        else:
            await send({'type': 'websocket.send',
                        'bytes': event.get('bytes') or b''})


# ---------------------------------------------------------------------------
# Entry point — invoked by launcher.py once per listener port.
# ---------------------------------------------------------------------------

def _parse_args():
    p = argparse.ArgumentParser(description='BlackBull on HttpArena')
    p.add_argument('--port', type=int, required=True)
    p.add_argument('--cert')
    p.add_argument('--key')
    p.add_argument('--workers', type=int, default=None)
    return p.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    # Expose the worker count for pool sizing — the launcher already
    # passes --workers (derived from cpu_count / WEB_WORKERS).
    if args.workers is not None:
        _WORKER_COUNT = max(args.workers, 1)
    # Match peer benchmark posture: access log off (apples-to-apples).
    os.environ.setdefault('BB_ACCESS_LOG', '0')
    # If logging_access.ini is present (placed by httparena_compare.sh when
    # BB_ACCESS_LOG=1), apply it now via the standard logging.config mechanism.
    # This is the single, declarative place that enables the blackbull.access
    # logger — no handler setup is scattered across launcher.py or library code.
    _logging_ini = os.path.join(os.path.dirname(__file__), 'logging_access.ini')
    if os.path.isfile(_logging_ini):
        import logging.config as _logging_config
        _logging_config.fileConfig(_logging_ini)
    # Database pool is initialised lazily on the first /async-db request.
    if args.cert and args.key:
        app.run(port=args.port, certfile=args.cert, keyfile=args.key,
                workers=args.workers)
    else:
        app.run(port=args.port, workers=args.workers)
