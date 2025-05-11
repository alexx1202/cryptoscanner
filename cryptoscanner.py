#!/usr/bin/env python3
import os, time, json, threading
import pandas as pd
import requests
import http.server, socketserver
from requests.exceptions import JSONDecodeError

# Configuration
BYBIT_API   = "https://api.bybit.com"
PERIODS     = ['1h','6h','12h','24h','7d','30d']
PORT        = int(os.environ.get('PORT', 8000))
PCT_METRICS = {'price_change','price_range','volume_change','funding_rate'}
METRICS     = ['price_change','price_range','volume_change','correlation','funding_rate']
REFRESH_INTERVAL = 60  # seconds between metric recomputations

def period_secs(p):
    unit, val = p[-1], int(p[:-1])
    return val * (3600 if unit=='h' else 86400)


def safe_json(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs).json()
    except (JSONDecodeError, ValueError):
        return {}

# API Calls

def get_top_pairs(limit=100):
    data = safe_json(requests.get, f"{BYBIT_API}/v5/market/tickers", params={'category':'linear'})
    all_ = data.get('result', {}).get('list', [])
    syms = [e['symbol'] for e in all_ if e.get('symbol','').endswith('USDT')]
    syms = [s for s in set(syms) if s!='BTCUSDT']
    syms = sorted(syms, key=lambda s: -float(next((e.get('turnover24h',0) for e in all_ if e['symbol']==s),0)))
    return syms[:limit]


def fetch_klines(sym, start, end, interval='60'):
    params = {
        'category':'linear', 'symbol': sym,
        'interval': interval, 'start': start,
        'end': end, 'limit': 200
    }
    res = safe_json(requests.get, f"{BYBIT_API}/v5/market/kline", params=params)
    raw = res.get('result', {}).get('list', [])
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume','turnover'])
    df['ts'] = pd.to_datetime(df['ts'].astype(float), unit='ms', errors='coerce')
    df.set_index('ts', inplace=True)
    for c in ['open','high','low','close','volume','turnover']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def fetch_funding(sym):
    data = safe_json(requests.get, f"{BYBIT_API}/v5/market/tickers", params={'category':'linear','symbol':sym})
    lst = data.get('result',{}).get('list',[])
    return float(next((e.get('fundingRate',0) for e in lst if e.get('symbol')==sym),0))

# Compute metrics
def compute_metric_df(sym_list, metric):
    df = pd.DataFrame(index=sym_list)
    now = int(time.time())
    for s in sym_list:
        for p in PERIODS:
            span = period_secs(p)
            start, end = now - span, now
            kl = fetch_klines(s, start, end, interval='60')
            val = None
            if metric == 'price_change' and not kl.empty:
                open_price = kl['open'].iloc[0]
                close_price = kl['close'].iloc[-1]
                val = (close_price - open_price) / open_price * 100
            elif metric == 'price_range' and not kl.empty:
                val = (kl['high'].max() - kl['low'].min()) / kl['low'].min() * 100
            elif metric == 'volume_change' and not kl.empty:
                cur = kl['volume'].sum()
                prev = fetch_klines(s, start - span, end - span, interval='60')['volume'].sum()
                if prev:
                    val = (cur - prev) / prev * 100
            elif metric == 'correlation' and len(kl) > 1:
                base = fetch_klines('BTCUSDT', start, end, interval='60')
                if len(base) > 1:
                    val = kl['close'].corr(base['close'])
            df.at[s, f"{metric}_{p}"] = val
        if metric == 'funding_rate':
            df.at[s, 'funding_rate'] = fetch_funding(s)
    return df

# Precompute and cache all metrics
cache = {}
tgt = ['BTCUSDT'] + get_top_pairs(99)

def refresh_all():
    global cache
    new = {}
    for m in METRICS:
        df = compute_metric_df(tgt, m)
        if m == 'funding_rate':
            cols = ['symbol', 'funding_rate']
        else:
            cols = ['symbol'] + [f"{m}_{p}" for p in PERIODS]
        rows = []
        for s in df.index:
            row = [s]
            for c in cols[1:]:
                v = df.at[s, c]
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    row.append(None)
                else:
                    if m in PCT_METRICS:
                        row.append(f"{v:.2f}%")
                    elif m == 'correlation':
                        row.append(f"{v:.2f}")
                    else:
                        row.append(v)
            rows.append(row)
        new[m] = {'columns': cols, 'rows': rows}
    cache = new

# kick off background updater
def updater_loop():
    while True:
        refresh_all()
        time.sleep(REFRESH_INTERVAL)

threading.Thread(target=updater_loop, daemon=True).start()

# HTTP server
class Handler(http.server.BaseHTTPRequestHandler):
    def _send_json(self, obj):
        b = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(b)

    def _send_html(self, html):
        b = html.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(b)

    def do_GET(self):
        path = self.path.rstrip('/')
        if path in ['', '/index.html']:
            self._send_html(self._menu_html())
        elif path.endswith('.json'):
            m = path[1:-5]
            if m in METRICS and m in cache:
                self._send_json(cache[m])
            else:
                self.send_error(404)
        elif path.endswith('.html'):
            m = path[1:-5]
            if m in METRICS:
                self._send_html(self._metric_html(m))
            else:
                self.send_error(404)
        else:
            self.send_error(404)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def _menu_html(self):
        links = ''.join(f"<li><a href='/{m}.html'>{m}</a></li>" for m in METRICS)
        return f"<html><body><h1>Metrics</h1><ul>{links}</ul></body></html>"

    def _metric_html(self, m):
        nav = ' | '.join(f"<a href='/{x}.html'>{x}</a>" for x in METRICS)
        return f'''
<html><head><meta charset="UTF-8"><title>{m}</title></head>
<body>
<p><a href="/index.html">Menu</a> | {nav}</p>
<div id="tbl"><p>Loading table...</p></div>
<script>
  async function refresh() {{
    document.getElementById('tbl').innerHTML = '<p>Loading data...</p>';
    let r = await fetch('/{m}.json');
    let d = await r.json();
    let html = '<table border="1"><tr>' + d.columns.map(c => '<th>'+c+'</th>').join('') + '</tr>'
      + d.rows.map(r => '<tr>'+ r.map(v => '<td>'+(v||'')+'</td>').join('') + '</tr>').join('')
      + '</table>';
    document.getElementById('tbl').innerHTML = html;
  }}
  setInterval(refresh, 5000);
  refresh();
</script>
</body></html>'''

if __name__=='__main__':
    print(f"Serving on http://0.0.0.0:{PORT}/index.html")
    with socketserver.TCPServer(('0.0.0.0', PORT), Handler) as srv:
        srv.serve_forever()
