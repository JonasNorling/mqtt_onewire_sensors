#!/usr/bin/env python3
#
# Receive sensor updates from Shelly H&T
#
import argparse
import logging
import math
from http.server import HTTPServer, BaseHTTPRequestHandler
import platform
from urllib.parse import urlparse, parse_qs

from mqtt_connection import MqttConnection

log = logging.getLogger('shelly_to_mqtt')
mqtt_connection: MqttConnection = None


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        ua = self.headers.get('User-Agent')
        log.debug(f'Request from {self.client_address}: {self.path} ({ua})')
        o = urlparse(self.path)
        qs = parse_qs(o.query)
        hum = float(qs.get('hum', [math.nan])[0])
        temp = float(qs.get('temp', [math.nan])[0])
        sensor_id = qs.get('id', [''])[0]
        log.info(f'Report from {sensor_id}: temp={temp:.2f}°C RH={hum:.1f}%')
        doc = f'{{"temperature":{temp:.2f}, "humidity":{hum:.1f}}}'
        mqtt_connection.publish(f'shelly/{sensor_id}', doc)
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        """Override from BaseHTTPRequestHandler"""
        message = format % args
        log.debug("%s - - [%s] %s" %
                  (self.address_string(),
                   self.log_date_time_string(),
                   message))


def run_http_server(port):
    httpd = HTTPServer(('', port), RequestHandler)
    log.info(f'Listening on port {port}')
    httpd.serve_forever()


def run():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Publish Shelly H&T sensors on MQTT')
    parser.add_argument('--port', default=7123, type=int, help='HTTP server port')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Enable debug printouts')
    MqttConnection.add_args(parser)
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    global mqtt_connection
    mqtt_connection = MqttConnection(f'shelly-{platform.node()}', args, log)

    try:
        mqtt_connection.start()
        run_http_server(args.port)
    except KeyboardInterrupt:
        log.info('Exit on CTRL-C')
    finally:
        mqtt_connection.stop()


if __name__ == "__main__":
    run()
