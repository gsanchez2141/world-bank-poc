"""World Bank loopback fixture with file-controlled failure and hold behavior."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import time
from urllib.parse import parse_qs, urlparse

CONTROL = Path('/fixture/control')

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path)
        query = parse_qs(path.query)
        page = int(query.get('page', ['1'])[0])
        mode = CONTROL.read_text().strip() if CONTROL.exists() else 'good'
        if mode == 'hold' and '/country/BRA/indicator/' in path.path and page == 1:
            deadline = time.monotonic() + 60
            while CONTROL.read_text().strip() == 'hold' and time.monotonic() < deadline:
                time.sleep(.2)
            mode = CONTROL.read_text().strip()
        if mode == 'bad' and '/country/BRA/indicator/' in path.path and page == 2:
            self.send_error(400)
            return
        if '/country/BRA/indicator/' in path.path:
            years = query.get('date', ['2020:2021'])[0].split(':')
            start, end = (int(value) for value in years)
            selected = [year for year in (2020, 2021) if start <= year <= end]
            if not 1 <= page <= len(selected):
                self.send_error(400)
                return
            year = selected[page - 1]
            row = {'countryiso3code': 'BRA', 'indicator': {'id': 'SP.POP.TOTL'},
                   'date': str(year), 'value': None if year == 2020 else (43 if mode == 'correction' else 42)}
            body = [{'page': page, 'pages': len(selected), 'total': len(selected)}, [row]]
        elif path.path.endswith('/country/BRA'):
            body = [{'page': 1, 'pages': 1}, [{'id':'BRA','name':'Brazil','incomeLevel':{'id':'UMC'}}]]
        elif path.path.endswith('/indicator/SP.POP.TOTL'):
            body = [{'page': 1, 'pages': 1}, [{'id':'SP.POP.TOTL','name':'Population, total'}]]
        elif path.path.endswith('/incomelevel'):
            body = [{'page': 1, 'pages': 1}, [{'id':'UMC','value':'Upper middle income'}]]
        else:
            self.send_error(404)
            return
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass

if __name__ == '__main__':
    ThreadingHTTPServer(('127.0.0.1', 18765), Handler).serve_forever()
