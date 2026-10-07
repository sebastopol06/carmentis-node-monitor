#!/usr/bin/env python3
import cbor2, base64, json, os, re, sqlite3, threading, time
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen
from urllib.parse import urlparse

RPC = os.getenv('RPC_URL', 'http://node-cometbft:26657').rstrip('/')
DB = os.getenv('DB_PATH', '/data/monitor.db')
KEEP_DAYS = int(os.getenv('KEEP_DAYS', '7'))
POLL_SECONDS = int(os.getenv('POLL_SECONDS', '10'))
BACKFILL_DAYS = int(os.getenv('BACKFILL_DAYS', '7'))
API_PORT = int(os.getenv('API_PORT', '8080'))
PRINTABLE = re.compile(rb'[\x20-\x7e]{4,}')
lock = threading.Lock()

def rpc(path):
    with urlopen(RPC + path, timeout=15) as r:
        return json.load(r)['result']

def conn():
    c=sqlite3.connect(DB, timeout=30)
    c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL')
    return c

def init_db():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    with conn() as c:
        c.executescript('''
        CREATE TABLE IF NOT EXISTS blocks(
          height INTEGER PRIMARY KEY, ts TEXT NOT NULL, bytes INTEGER NOT NULL,
          tx_count INTEGER NOT NULL, useful_count INTEGER NOT NULL,
          commercial_count INTEGER NOT NULL, reward_count INTEGER NOT NULL,
          other_count INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS txs(
          height INTEGER NOT NULL, idx INTEGER NOT NULL, ts TEXT NOT NULL,
          size INTEGER NOT NULL, kind TEXT NOT NULL, notable TEXT,
          PRIMARY KEY(height,idx));
        CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT NOT NULL);
        ''')        
        # Migration: native Carmentis type 0..5
        cols={r['name'] for r in c.execute('PRAGMA table_info(txs)')}
        if 'mb_type' not in cols:
            c.execute('ALTER TABLE txs ADD COLUMN mb_type INTEGER')

def text_of(raw):
    return ' '.join(x.decode('ascii','ignore') for x in PRINTABLE.findall(raw))

def microblock_type(raw):
    """Native Carmentis microblock type (0..5), decoded from CBOR."""
    try:
        obj = cbor2.loads(raw)
        t = int(obj['header']['microblockType'])
        return t if 0 <= t <= 5 else None
    except Exception:
        return None

def classify(raw):
    s=text_of(raw)
    low=s.lower()
    if 'carmentis incentive program' in low and 'reward payment' in low:
        return 'reward', 'incentive reward'
    # Known node/account onboarding/admin vocabulary: excluded from useful/commercial usage.
    admin_terms=('node-operator-', 'rpcendpoint', 'cometpublickey', 'organizationid',
                 'countrycode', 'selleraccount')
    if any(x in low for x in admin_terms):
        return 'admin', 'network/account administration'
    # Strong application/business hints. This list can be refined as real payloads appear.
    commercial_terms=('applicationledger','application ledger','credential','document',
                      'invoice','facture','loan','recovery','proof','certificate','verifiable')
    hits=[x for x in commercial_terms if x in low]
    if hits:
        return 'commercial', ', '.join(hits[:3])
    return 'other', (s[:180] if s else 'unclassified payload')

def block(height):
    r=rpc('/block?height=%d' % height)
    b=r['block']; tx64=(b.get('data') or {}).get('txs') or []
    ts=b['header']['time']
    decoded=[]
    total_bytes=0
    for i,t in enumerate(tx64):
        try: raw=base64.b64decode(t)
        except Exception: raw=b''
        total_bytes += len(raw)
        kind,note=classify(raw)
        mb_type=microblock_type(raw)
        decoded.append((i,len(raw),kind,note,mb_type))
    return ts,total_bytes,decoded

def store_block(h):
    ts,nbytes,txs=block(h)
    counts={k:0 for k in ('reward','admin','commercial','other')}
    for _,_,k,_,_ in txs: counts[k]+=1
    # useful = everything not reward/admin. "other" is kept visible rather than silently called commercial.
    useful=counts['commercial']+counts['other']
    with conn() as c:
        c.execute('INSERT OR IGNORE INTO blocks VALUES(?,?,?,?,?,?,?,?)',
          (h,ts,nbytes,len(txs),useful,counts['commercial'],counts['reward'],counts['other']))
        c.executemany('INSERT OR IGNORE INTO txs(height,idx,ts,size,kind,notable,mb_type) VALUES(?,?,?,?,?,?,?)',
          [(h,i,ts,size,k,note,mb_type) for i,size,k,note,mb_type in txs])
        c.execute("INSERT OR REPLACE INTO meta VALUES('last_height',?)",(str(h),))

def estimate_start(latest):
    # Uses observed ~20 s blocks only for first bootstrap; then scanning is exactly incremental.
    return max(1, latest - BACKFILL_DAYS*24*60*3)

def scanner():
    while True:
        try:
            status = rpc('/status')['sync_info']
            latest = int(status['latest_block_height'])
            earliest = int(status['earliest_block_height'])
            with conn() as c:
                row = c.execute(
                    "SELECT v FROM meta WHERE k='last_height'"
                ).fetchone()
            if row:
                nxt = int(row['v']) + 1
            else:
                nxt = max(earliest, estimate_start(latest))
            while nxt <= latest:
                store_block(nxt)
                nxt += 1
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)
            ).isoformat().replace('+00:00', 'Z')
            with conn() as c:
                c.execute('DELETE FROM txs WHERE ts < ?', (cutoff,))
                c.execute('DELETE FROM blocks WHERE ts < ?', (cutoff,))
        except Exception as e:
            print('scanner:', repr(e), flush=True)
        time.sleep(POLL_SECONDS)

def percentile(vals,p):
    if not vals:return 0
    vals=sorted(vals); i=min(len(vals)-1,max(0,int((len(vals)-1)*p)))
    return vals[i]

def stats():
    with conn() as c:
        a=c.execute('''SELECT COUNT(*) blocks, COALESCE(SUM(tx_count),0) tx,
          COALESCE(SUM(reward_count),0) rewards, COALESCE(SUM(commercial_count),0) commercial,
          COALESCE(SUM(other_count),0) other, COALESCE(SUM(useful_count),0) useful,
          COALESCE(SUM(bytes),0) bytes, MIN(ts) since, MAX(ts) until,
          COALESCE(MAX(tx_count),0) max_tx_block, COALESCE(MAX(bytes),0) max_bytes_block
          FROM blocks''').fetchone()
        # 5-minute buckets for empirical CDF / peak-load distribution.
        rows=c.execute('''SELECT CAST(strftime('%s',ts)/300 AS INTEGER) bucket,
          SUM(tx_count) tx, SUM(useful_count) useful, SUM(commercial_count) commercial, SUM(bytes) bytes
          FROM blocks GROUP BY bucket ORDER BY bucket''').fetchall()
        notable=c.execute("SELECT kind,notable,COUNT(*) n FROM txs WHERE kind NOT IN ('reward','admin') GROUP BY kind,notable ORDER BY n DESC LIMIT 20").fetchall()
    tx5=[r['tx'] for r in rows]; useful5=[r['useful'] for r in rows]; bytes5=[r['bytes'] for r in rows]
    d=dict(a)
    d['cdf_5m']={
      'tx':{'p50':percentile(tx5,.50),'p90':percentile(tx5,.90),'p99':percentile(tx5,.99),'max':max(tx5,default=0)},
      'useful':{'p50':percentile(useful5,.50),'p90':percentile(useful5,.90),'p99':percentile(useful5,.99),'max':max(useful5,default=0)},
      'bytes':{'p50':percentile(bytes5,.50),'p90':percentile(bytes5,.90),'p99':percentile(bytes5,.99),'max':max(bytes5,default=0)}}
    d['notable']=[dict(x) for x in notable]
    try:
        cp=rpc('/consensus_params')
        bp=cp.get('consensus_params',{}).get('block',{})
        d['consensus_limits']={'max_bytes':int(bp.get('max_bytes','0')), 'max_gas':int(bp.get('max_gas','-1'))}
        mb=d['consensus_limits']['max_bytes']
        d['peak_vs_max_bytes_pct']=round(100*d['max_bytes_block']/mb,5) if mb>0 else None
    except Exception:
        d['consensus_limits']=None; d['peak_vs_max_bytes_pct']=None
    return d

def chart_data():
    """Hourly activity + block-load distribution over retained 7-day window."""
    with conn() as c:
        # Existing hourly activity
        # Hourly activity
        hourly = c.execute("""
            SELECT
                strftime('%Y-%m-%dT%H:00:00Z', ts) AS hour,
                COUNT(*) AS blocks,
                COALESCE(SUM(tx_count), 0) AS tx,
                COALESCE(SUM(reward_count), 0) AS rewards,
                COALESCE(SUM(commercial_count), 0) AS commercial,
                COALESCE(SUM(other_count), 0) AS other,
                COALESCE(SUM(useful_count), 0) AS useful,
                COALESCE(SUM(bytes), 0) AS bytes,
                COALESCE(MAX(bytes), 0) AS peak_block_bytes
            FROM blocks
            GROUP BY hour
            ORDER BY hour
        """).fetchall()
        # Distribution of block sizes
        block_sizes = [
            r[0] for r in c.execute(
                "SELECT bytes FROM blocks ORDER BY bytes"
            ).fetchall()
        ]
        # Native Carmentis activity: 5-minute rolling-window buckets.
        native = c.execute("""
            SELECT
                CAST(strftime('%s', b.ts) / 3600 AS INTEGER) AS bucket,
                COUNT(DISTINCT b.height) AS blocks,
                COALESCE(SUM(CASE WHEN t.mb_type=0 THEN t.size ELSE 0 END),0) AS type0,
                COALESCE(SUM(CASE WHEN t.mb_type=1 THEN t.size ELSE 0 END),0) AS type1,
                COALESCE(SUM(CASE WHEN t.mb_type=2 THEN t.size ELSE 0 END),0) AS type2,
                COALESCE(SUM(CASE WHEN t.mb_type=3 THEN t.size ELSE 0 END),0) AS type3,
                COALESCE(SUM(CASE WHEN t.mb_type=4 THEN t.size ELSE 0 END),0) AS type4,
                COALESCE(SUM(CASE WHEN t.mb_type=5 THEN t.size ELSE 0 END),0) AS type5
            FROM blocks b
            LEFT JOIN txs t ON t.height=b.height
            GROUP BY bucket
            ORDER BY bucket
        """).fetchall()
    try:
        params = rpc('/consensus_params')
        max_bytes = int(
            params['consensus_params']['block']['max_bytes']
        )
    except Exception:
        max_bytes = 0
    activity = []
    for r in hourly:
        activity.append({
            "hour": r["hour"],
            "blocks": r["blocks"],
            "tx": r["tx"],
            "rewards": r["rewards"],
            "commercial": r["commercial"],
            "other": r["other"],
            "useful": r["useful"],
            "bytes": r["bytes"],
            "peak_block_bytes": r["peak_block_bytes"],
            "peak_capacity_pct":
                round(100 * r["peak_block_bytes"] / max_bytes, 6)
                if max_bytes else 0
        })
    # CDF: max 200 representative points
    cdf = []
    n = len(block_sizes)
    if n:
        points = min(200, n)
        for i in range(points):
            idx = round(i * (n - 1) / (points - 1)) if points > 1 else 0
            size = block_sizes[idx]
            cdf.append({
                "percentile": round(100 * idx / (n - 1), 2)
                    if n > 1 else 100,
                "bytes": size,
                "capacity_pct":
                    round(100 * size / max_bytes, 8)
                    if max_bytes else 0
            })
    native_activity = []
    for r in native:
        capacity = max_bytes * r["blocks"]
        native_activity.append({
           "ts": r["bucket"] * 3600,
           "type0": r["type0"],
           "type1": r["type1"],
           "type2": r["type2"],
           "type3": r["type3"],
           "type4": r["type4"],
           "type5": r["type5"],
           "max_theoretical": max_bytes * r["blocks"]
        })
    return {
        "window_days": KEEP_DAYS,
        "max_bytes": max_bytes,
        "activity": activity,
        "cdf": cdf,
        "native_activity": native_activity
    }

class Handler(BaseHTTPRequestHandler):
    def sendj(self,obj,code=200):
        b=json.dumps(obj,separators=(',',':')).encode()
        self.send_response(code); self.send_header('Content-Type','application/json')
        self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        p=urlparse(self.path).path
        if p in ('/health','/api/health'): self.sendj({'ok':True}); return
        if p in ('/stats','/api/stats'):
            try:self.sendj(stats())
            except Exception as e:self.sendj({'error':str(e)},500)
            return
        if p in ('/charts','/api/charts'):
            try:self.sendj(chart_data())
            except Exception as e:self.sendj({'error':str(e)},500)
            return
        self.sendj({'error':'not found'},404)
    def log_message(self,fmt,*args): pass

if __name__=='__main__':
    init_db()
    threading.Thread(target=scanner,daemon=True).start()
    print('Carmentis monitor API on :%d, RPC=%s' % (API_PORT,RPC), flush=True)
    ThreadingHTTPServer(('0.0.0.0',API_PORT),Handler).serve_forever()
