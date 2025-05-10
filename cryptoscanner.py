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

# Configuration
BYBIT_API = 'https://api.bybit.com'
INTERVAL = '60'
PERIODS = ['1h','6h','12h','24h','7d','30d']
PORT = int(os.environ.get('PORT', 8000))

# Helpers
def period_secs(p):
    unit, val = p[-1], int(p[:-1])
    return val * (3600 if unit=='h' else 86400)

def safe_json(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs).json()
    except (JSONDecodeError, ValueError):
        return {}

# API functions
def get_top_pairs(limit=100):
    data = safe_json(requests.get, f"{BYBIT_API}/v5/market/tickers", params={'category':'linear'})
    entries = data.get('result', {}).get('list', [])
    syms = [e['symbol'] for e in entries if e.get('symbol','').endswith('USDT')]
    syms = [s for s in syms if float(next((e.get('turnover24h',0) for e in entries if e['symbol']==s),0))>1000]
    def turnover(s):
        return float(next((e.get('turnover24h',0) for e in entries if e['symbol']==s),0))
    return sorted(set(syms), key=lambda s: -turnover(s))[:limit]

def fetch_klines(sym, start, end):
    data = safe_json(requests.get, f"{BYBIT_API}/v5/market/kline",
                     params={'category':'linear','symbol':sym,'interval':INTERVAL,'start':start,'end':end,'limit':200})
    raw = data.get('result', {}).get('list', [])
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume','turnover'])
    df['ts'] = pd.to_datetime(pd.to_numeric(df['ts'], errors='coerce'), unit='ms', errors='coerce')
    df.set_index('ts', inplace=True)
    for c in ['open','high','low','close','volume','turnover']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

def fetch_funding(sym):
    data = safe_json(requests.get, f"{BYBIT_API}/v5/market/tickers",
                     params={'category':'linear','symbol':sym})
    lst = data.get('result', {}).get('list', [])
    return float(next((e.get('fundingRate',0) for e in lst if e['symbol']==sym),0))

# Compute metric dataframe
def compute_metric_df(sym_list, metric):
    df = pd.DataFrame(index=sym_list)
    now_ms = int(time.time() * 1000)
    for s in sym_list:
        for p in PERIODS:
            span = period_secs(p) * 1000
            start, end = now_ms - span, now_ms
            col = f"{metric}_{p}"
            kl = fetch_klines(s, start, end)
            val = None
            if metric == 'price_change' and 'close' in kl and len(kl['close']) > 1:
                val = (kl['close'].iloc[-1] - kl['close'].iloc[0]) / kl['close'].iloc[0] * 100
            elif metric == 'price_range' and {'high','low'}.issubset(kl.columns) and not kl.empty:
                val = (kl['high'].max() - kl['low'].min()) / kl['low'].min() * 100
            elif metric == 'volume_change':
                cur = kl.get('volume', pd.Series(dtype=float)).sum()
                prev = fetch_klines(s, start-span, end-span).get('volume', pd.Series(dtype=float)).sum()
                if prev:
                    val = (cur - prev) / prev * 100
            elif metric == 'correlation' and 'close' in kl and len(kl['close']) > 1:
                base = fetch_klines('BTCUSDT', start, end)
                if 'close' in base and len(base['close']) > 1:
                    val = kl['close'].corr(base['close'])
            df.at[s, col] = val
        if metric == 'funding_rate':
            df.at[s, 'funding_rate'] = fetch_funding(s)
    return df

# Globals
SYMS = ['BTCUSDT'] + get_top_pairs(99)
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
                out.append(f'<li><a href="/{m}.json">{m} JSON</a> | <a href="/{m}.html">{m} HTML</a></li>')
            out.append('</ul></body></html>')
            self.wfile.write('\n'.join(out).encode())
        elif path.endswith('.json') and any(path == f'/{m}.json' for m in METRICS):
            m = path[1:-5]
            df = compute_metric_df(SYMS, m)
            cols = ['symbol'] + ([f"{m}_{p}" for p in PERIODS] if m != 'funding_rate' else ['funding_rate'])
            # Build and sanitize rows
            rows = []
            for s in df.index:
                row = [s]
                for c in cols[1:]:
                    v = df.at[s, c]
                    # unwrap pandas Series
                    if isinstance(v, pd.Series):
                        v = v.iloc[0] if len(v) == 1 else v.tolist()
                    # numpy scalars to Python types
                    try:
                        if hasattr(v, 'item'):
                            v = v.item()
                    except Exception:
                        pass
                    # pandas NA to None
                    if pd.isna(v):
                        v = None
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
            script = [
                '<script>',
                'async function refresh() {',
                f'  const r = await fetch("/{m}.json");',
                '  const d = await r.json();',
                '  let t = `<tr>${d.columns.map(c => `<th>${c}</th>`).join("")}</tr>`;',
                '  t += d.rows.map(r => `<tr>${r.map(v => `<td>${v}</td>`).join("")}</tr>`).join("");',
                '  document.getElementById("tbl").innerHTML = t;',
                '}',
                'setInterval(refresh, 1000);',
                '</script>'
            ]
            html = ['<html><head><meta charset="UTF-8"><title>' + m + '</title></head><body>',
                    '<p><a href="/index.html">Menu</a> | ' + nav + '</p>',
                    '<table id="tbl" border="1"></table>'] + script + ['<script>refresh();</script></body></html>']
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
