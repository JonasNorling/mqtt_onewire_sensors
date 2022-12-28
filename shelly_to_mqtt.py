#!/usr/bin/env python3
#
# Receive sensor updates from Shelly H&T
#

import logging
import math
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

log = logging.getLogger("shelly_to_mqtt")


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        ua = self.headers.get('User-Agent')
        log.debug(f'Request from {self.client_address}: {self.path} ({ua})')
        o = urlparse(self.path)
        qs = parse_qs(o.query)
        hum = float(qs.get('hum', [math.nan])[0])
        temp = float(qs.get('temp', [math.nan])[0])
        sensor_id = qs.get('id', [''])[0]
        log.info(f'Report from {sensor_id}: temp={temp:.2f}°C RH={hum:.0f}%')
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        """Override from BaseHTTPRequestHandler"""
        message = format % args
        log.debug("%s - - [%s] %s" %
                  (self.address_string(),
                   self.log_date_time_string(),
                   message.translate(self._control_char_table)))


def run_server():
    server_address = ('', 7123)
    httpd = HTTPServer(server_address, RequestHandler)
    log.info(f'Listening on {server_address}')
    httpd.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_server()
