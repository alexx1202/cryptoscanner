#!/usr/bin/env python3
import sys
import os
import math
import json
import time
import pandas as pd
import requests
from requests.exceptions import JSONDecodeError
import http.server, socketserver
from threading import Lock

# Configuration
BYBIT_API = 'https://api.bybit.com'
INTERVAL = '1h'  # use human-readable for code, convert to "60" for API
SYMS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
PERIODS = ['1h', '6h', '12h']
PORT = int(os.environ.get('PORT', 8000))
CACHE = {}
CACHE_LOCK = Lock()
CACHE_TTL = 10  # seconds

# Helpers
def period_secs(p):
    unit, val = p[-1], int(p[:-1])
    return val * (3600 if unit=='h' else 86400)

def interval_to_seconds(interval_str):
    if interval_str.endswith('m'):
        return int(interval_str[:-1]) * 60
    elif interval_str.endswith('h'):
        return int(interval_str[:-1]) * 3600
    elif interval_str.endswith('d'):
        return int(interval_str[:-1]) * 86400
    else:
        raise ValueError("Unsupported interval: " + interval_str)

def safe_json(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs).json()
    except (JSONDecodeError, ValueError):
        return {}

def get_metric_df_cached(metric):
    now = time.time()
    with CACHE_LOCK:
        if metric in CACHE and (now - CACHE[metric]['ts'] < CACHE_TTL):
            return CACHE[metric]['df']
    df = compute_metric_df(SYMS, metric)
    with CACHE_LOCK:
        CACHE[metric] = {'df': df, 'ts': now}
    return df

# API functions
def fetch_klines(sym, start, end):
    # Convert INTERVAL to bybit-style string for API
    interval_api = '60' if INTERVAL == '1h' else INTERVAL
    data = safe_json(requests.get, f"{BYBIT_API}/v5/market/kline",
                     params={'category':'linear','symbol':sym,'interval':interval_api,'start':start,'end':end,'limit':200})
    raw = data.get('result', {}).get('list', [])
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume','turnover'])
    df['ts'] = pd.to_datetime(pd.to_numeric(df['ts'], errors='coerce'), unit='ms', errors='coerce')
    df.set_index('ts', inplace=True)
    for c in ['open','high','low','close','volume','turnover']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

def fetch_all_klines(sym, start, end, interval_sec):
    dfs = []
    cur = start
    while cur < end:
        chunk_end = min(cur + 200 * interval_sec * 1000, end)
        df = fetch_klines(sym, cur, chunk_end)
        if df.empty:
            break
        dfs.append(df)
        cur = int(df.index[-1].timestamp() * 1000) + interval_sec * 1000
        if cur <= chunk_end:
            break
    if dfs:
        out = pd.concat(dfs).sort_index()
        out = out[~out.index.duplicated(keep='first')]
        return out
    return pd.DataFrame()

def fetch_funding(sym):
    data = safe_json(requests.get, f"{BYBIT_API}/v5/market/tickers",
                     params={'category':'linear','symbol':sym})
    lst = data.get('result', {}).get('list', [])
    return float(next((e.get('fundingRate',0) for e in lst if e['symbol']==sym),0))

# Compute metric dataframe
def compute_metric_df(sym_list, metric):
    df = pd.DataFrame(index=sym_list)
    now_ms = int(time.time() * 1000)
    interval_sec = interval_to_seconds(INTERVAL)
    for s in sym_list:
        for p in PERIODS:
            span = period_secs(p) * 1000
            start, end = now_ms - span, now_ms
            col = f"{metric}_{p}"
            kl = fetch_all_klines(s, start, end, interval_sec)
            val = None
            if metric == 'price_change' and 'close' in kl and len(kl['close']) > 1:
                val = (kl['close'].iloc[-1] - kl['close'].iloc[0]) / kl['close'].iloc[0] * 100
            elif metric == 'price_range' and {'high','low'}.issubset(kl.columns) and not kl.empty:
                val = (kl['high'].max() - kl['low'].min()) / kl['low'].min() * 100
            elif metric == 'volume_change':
                cur = kl.get('volume', pd.Series(dtype=float)).sum()
                prev_kl = fetch_all_klines(s, start - span, end - span, interval_sec)
                prev = prev_kl.get('volume', pd.Series(dtype=float)).sum()
                if prev:
                    val = (cur - prev) / prev * 100
            elif metric == 'correlation' and 'close' in kl and len(kl['close']) > 1:
                base = fetch_all_klines('BTCUSDT', start, end, interval_sec)
                if 'close' in base and len(base['close']) > 1:
                    val = kl['close'].corr(base['close'])
            df.at[s, col] = val
        if metric == 'funding_rate':
            df.at[s, 'funding_rate'] = fetch_funding(s)
    return df

METRICS = ['price_change','price_range','volume_change','correlation','funding_rate']

# HTTP Handler
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.rstrip('/')
        if path == '' or path == '/index.html':
            self.send_response(200)
            self.send_header('Content-Type','text/html')
            self.end_headers()
            out = ['<html><head><meta charset="UTF-8"><title>Metrics</title></head><body>', '<h1>Select a Metric</h1><ul>']
            for m in METRICS:
                out.append(f'<li><a href="/{m}.html">{m} HTML</a></li>')
            out.append('</ul></body></html>')
            self.wfile.write('\n'.join(out).encode())
        elif path.endswith('.json') and any(path == f'/{m}.json' for m in METRICS):
            m = path[1:-5]
            df = get_metric_df_cached(m)
            cols = ['symbol'] + ([f"{m}_{p}" for p in PERIODS] if m != 'funding_rate' else ['funding_rate'])
            rows = []
            for s in df.index:
                row = [s]
                for c in cols[1:]:
                    v = df.at[s, c]
                    # unwrap pandas Series
                    if isinstance(v, pd.Series):
                        v = v.iloc[0] if len(v) == 1 else v.tolist()
                    # numpy scalar
                    if hasattr(v, 'item') and not isinstance(v, list):
                        v = v.item()
                    # clean list elements
                    if isinstance(v, list):
                        cleaned = []
                        for x in v:
                            if pd.isna(x):
                                cleaned.append(None)
                            else:
                                if hasattr(x, 'item'):
                                    try: x = x.item()
                                    except: pass
                                cleaned.append(x)
                        v = cleaned
                    # pandas NA or single nan
                    elif pd.isna(v):
                        v = None
                    # format as percent for appropriate metrics
                    if v is not None and isinstance(v, (float, int)):
                        if (('change' in c) or ('range' in c)):
                            v = f"{v:.2f}%"
                        elif c == 'funding_rate':
                            v = f"{v*100:.4f}%"
                        elif 'correlation' in c:
                            v = f"{v*100:.2f}%"
                    row.append(v)
                rows.append(row)
            payload = {'columns': cols, 'rows': rows}
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())
        elif path.endswith('.html') and any(path == f'/{m}.html' for m in METRICS):
            m = path[1:-5]
            self.send_response(200)
            self.send_header('Content-Type','text/html')
            self.end_headers()
            nav = ' | '.join([f'<a href="/{x}.html">{x}</a>' for x in METRICS])
            # New JS: loading spinner, error message, periodic refresh
            script = [
                '<script>',
                'let loading = true;',
                'let timer = null;',
                'function showLoading() {',
                '  document.getElementById("tbl").innerHTML = "<tr><td colspan=\'99\' style=\'text-align:center\'><span id=\'spin\'>⏳ Loading...</span></td></tr>";',
                '}',
                'function showError(err) {',
                '  document.getElementById("tbl").innerHTML = "<tr><td colspan=\'99\' style=\'color:red;text-align:center\'>⚠️ Error: "+err+"</td></tr>";',
                '}',
                'async function refresh() {',
                '  try {',
                '    showLoading();',
                '    const r = await fetch(location.pathname.replace(".html", ".json"));',
                '    if (!r.ok) throw new Error(r.status + " " + r.statusText);',
                '    const d = await r.json();',
                '    if (!d.rows.length) throw new Error("No data (API rate limit or server error)");',
                '    let t = `<tr>${d.columns.map(c => `<th>${c}</th>`).join("")}</tr>`;',
                '    t += d.rows.map(r => `<tr>${r.map(v => `<td>${v ?? ""}</td>`).join("")}</tr>`).join("");',
                '    document.getElementById("tbl").innerHTML = t;',
                '  } catch(e) {',
                '    showError(e.message || e);',
                '  }',
                '  loading = false;',
                '}',
                'window.onload = function() {',
                '  showLoading();',
                '  refresh();',
                '  timer = setInterval(refresh, 30000);',
                '}',
                '</script>'
            ]
            html = ['<html><head><meta charset="UTF-8"><title>' + m + '</title></head><body>',
                    '<p><a href="/index.html">Menu</a> | ' + nav + '</p>',
                    '<table id="tbl" border="1"></table>'] + script + ['</body></html>']
            self.wfile.write('\n'.join(html).encode())
        else:
            self.send_error(404)

    def do_HEAD(self):
        path = self.path.rstrip('/')
        if path == '' or path == '/index.html':
            self.send_response(200)
            self.send_header('Content-Type','text/html')
            self.end_headers()
        elif path.endswith('.json') and any(path == f'/{m}.json' for m in METRICS):
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.end_headers()
        elif path.endswith('.html') and any(path == f'/{m}.html' for m in METRICS):
            self.send_response(200)
            self.send_header('Content-Type','text/html')
            self.end_headers()
        else:
            self.send_error(404)

# Main
if __name__ == '__main__':
    try:
        with socketserver.TCPServer(('0.0.0.0', PORT), Handler) as server:
            print(f"Serving on http://0.0.0.0:{PORT}/index.html")
            server.serve_forever()
    except OSError as e:
        print(f"Server not supported in this environment ({e}). Please deploy to Render.com or a full Python environment.")
        pass
