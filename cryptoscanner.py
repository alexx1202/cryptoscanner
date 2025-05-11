#!/usr/bin/env python3
import os, time, json
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

# Helpers

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
    syms = [s for s in syms if float(next((e.get('turnover24h',0) for e in all_ if e['symbol']==s),0))>1000]
    syms = sorted(set(syms), key=lambda s: -float(next((e.get('turnover24h',0) for e in all_ if e['symbol']==s),0)))
    syms = [s for s in syms if s!='BTCUSDT']
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
    df['ts'] = pd.to_datetime(df['ts'].astype(float, errors='ignore'), unit='ms', errors='coerce')
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
            start, end = now-span, now
            iv = '1' if p=='1h' else '60'
            kl = fetch_klines(s, start, end, interval=iv)
            val = None
            if metric=='price_change' and len(kl)>1:
                val = (kl['close'].iloc[-1] - kl['close'].iloc[0]) / kl['close'].iloc[0] * 100
            elif metric=='price_range' and {'high','low'}.issubset(kl.columns):
                val = (kl['high'].max() - kl['low'].min()) / kl['low'].min() * 100
            elif metric=='volume_change':
                cur = kl['volume'].sum() if 'volume' in kl else 0
                prev = 0 if span==0 else fetch_klines(s, start-span, end-span, interval=iv)['volume'].sum()
                if prev:
                    val = (cur-prev)/prev*100
            elif metric=='correlation' and len(kl)>1:
                base = fetch_klines('BTCUSDT', start, end, interval=iv)
                if len(base)>1:
                    val = kl['close'].corr(base['close'])
            df.at[s, f"{metric}_{p}"] = val
        if metric=='funding_rate':
            df.at[s, 'funding_rate'] = fetch_funding(s)
    return df

# Server

tgt = ['BTCUSDT'] + get_top_pairs(99)

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.rstrip('/')
        if path in ['', '/index.html']:
            self._send_html(self._menu_html())
        elif path.endswith('.json'):
            m = path[1:-5]
            if m in METRICS:
                df = compute_metric_df(tgt, m)
                cols = ['symbol'] + ([f"{m}_{p}" for p in PERIODS] if m!='funding_rate' else ['funding_rate'])
                rows = []
                for s in df.index:
                    row = [s]
                    for c in cols[1:]:
                        v = df.at[s,c]
                        if pd.isna(v):
                            v = None
                        else:
                            if m in PCT_METRICS:
                                v = f"{v:.2f}%"
                            elif m=='correlation':
                                v = f"{v:.2f}"
                        row.append(v)
                    rows.append(row)
                self._send_json({'columns': cols, 'rows': rows})
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
        if self.command=='HEAD':
            self.do_GET()
        else:
            self.send_error(405)

    def _send_json(self, obj):
        b = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        if self.command!='HEAD':
            self.wfile.write(b)

    def _send_html(self, html):
        b = html.encode()
        self.send_response(200)
        self.send_header('Content-Type','text/html')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        if self.command!='HEAD':
            self.wfile.write(b)

    def _menu_html(self):
        links = ''.join(f"<li><a href='/{m}.html'>{m}</a></li>" for m in METRICS)
        return f"<html><body><h1>Metrics</h1><ul>{links}</ul></body></html>"

    def _metric_html(self, m):
        nav = ' | '.join(f"<a href='/{x}.html'>{x}</a>" for x in METRICS)
        return f'''
<html><head><meta charset="UTF-8"><title>{m}</title></head>
<body>
<p><a href="/index.html">Menu</a> | {nav}</p>
<div id="tbl">Loading table...</div>
<script>
  async function refresh() {{
    let r = await fetch('/{m}.json');
    let d = await r.json();
    let html = '<table border="1"><tr>'
      + d.columns.map(c => '<th>'+c+'</th>').join('') + '</tr>'
      + d.rows.map(r => '<tr>'+ r.map(v => '<td>'+ (v || '') + '</td>').join('') + '</tr>').join('')
      + '</table>';
    document.getElementById('tbl').innerHTML = html;
  }}
  setInterval(refresh, 1000);
  refresh();
</script>
</body></html>'''

if __name__=='__main__':
    with socketserver.TCPServer(('0.0.0.0', PORT), Handler) as srv:
        print(f"Serving on http://0.0.0.0:{PORT}/index.html")
        srv.serve_forever()
