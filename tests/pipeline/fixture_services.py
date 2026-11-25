"""World Bank loopback fixture with file-controlled failure and hold behavior."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import time
from urllib.parse import parse_qs, urlsplit, unquote

CONTROL = Path('/fixture/control')

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlsplit(self.path)
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
        if mode.startswith('dq_'):
            parts = unquote(path.path).strip('/').split('/')
            if 'country' in parts and 'indicator' in parts:
                countries = parts[parts.index('country') + 1].split(';')
                indicator = parts[parts.index('indicator') + 1]
                years = query.get('date', ['2020:2021'])[0].split(':')
                start, end = (int(value) for value in years)
                selected = [year for year in (2020, 2021) if start <= year <= end]
                if not 1 <= page <= len(selected):
                    self.send_error(400)
                    return
                year = selected[page-1]
                records = []
                for country in countries:
                    if mode == 'dq_omit' and country == 'BRA' and year == 2021 and indicator in ('NY.GDP.MKTP.CD','NY.GDP.PCAP.CD'):
                        continue
                    values = {'SP.POP.TOTL':100,'NY.GDP.MKTP.CD':1000,'NY.GDP.PCAP.CD':10}
                    value = values[indicator]
                    if country == 'BRA' and year == 2020 and indicator == 'SP.POP.TOTL':
                        value = None if mode != 'dq_numeric' else 100
                    if country == 'BRA' and year == 2021 and mode == 'dq_numeric':
                        value = {'SP.POP.TOTL':120,'NY.GDP.MKTP.CD':1320,'NY.GDP.PCAP.CD':11}[indicator]
                    records.append({'countryiso3code':country,'indicator':{'id':indicator},'date':str(year),'value':value})
                body = [{'page':page,'pages':len(selected),'total':len(countries)*len(selected)},records]
            elif 'country' in parts:
                countries=parts[parts.index('country')+1].split(';')
                body=[{'page':1,'pages':1},[{'id':c,'name':c,'incomeLevel':{'id':'UMC'}} for c in countries]]
            elif 'indicator' in parts:
                indicator=parts[parts.index('indicator')+1]
                body=[{'page':1,'pages':1},[{'id':indicator,'name':indicator}]]
            elif path.path.endswith('/incomelevel'):
                body=[{'page':1,'pages':1},[{'id':'UMC','value':'Upper middle income'}]]
            else:
                self.send_error(404)
                return
            data=json.dumps(body).encode()
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(data)))
            self.end_headers()
            self.wfile.write(data)
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
