#!/usr/bin/env python3
#
# Pull temperature readings from MQTT and add to a SQL database
#

import argparse
import logging
import platform
from contextlib import suppress
from typing import Dict

import paho.mqtt.client as mqtt
import time
import re
import json
import sqlite3

TOPIC_MATCH = 'zigbee2mqtt/+'
TOPIC_RE = re.compile(TOPIC_MATCH.replace('#', r'(.*)').replace('+', r'([^/]*)'))

db: sqlite3.Connection = None
series_ids: Dict[str, int] = {}
last_samples: Dict[str, int] = {}


def get_series_id(name):
    with suppress(KeyError):
        return series_ids[name]
    cur = db.execute('SELECT id FROM series WHERE name=?', (name, ))
    result = cur.fetchone()
    if result is not None:
        id = result[0]
        series_ids[name] = id
        return id
    cur = db.execute('INSERT INTO series (name) VALUES (?)', (name, ))
    return cur.lastrowid


def update_db(timestamp: int, source_name: str, value: float) -> bool:
    log.debug(f'Reading at {timestamp} for {source_name}: {value:.3f}')
    with suppress(KeyError):
        if last_samples[source_name] == timestamp:
            log.debug("Suppressing duplicate reading")
            return False
    last_samples[source_name] = timestamp
    series_id = get_series_id(source_name)
    db.execute('INSERT INTO samples (time, series, value) VALUES (?, ?, ?)',
               (timestamp, series_id, value))
    return True


def on_connect(client, userdata, flags, rc):
    log.info(f'Connected: {rc}')
    client.subscribe("$SYS/broker/version")
    client.subscribe(TOPIC_MATCH)


def on_message(client, userdata, msg):
    log.debug(f'Message: {msg.topic}: {msg.payload}')
    match = TOPIC_RE.match(msg.topic)
    if match:
        try:
            updated = False
            node_name = match.group(1)
            content = json.loads(msg.payload)
            if 'temperature' in content:
                value = content['temperature']
                source_name = f'{node_name}-t'
                updated |= update_db(int(time.time()), source_name, value)
            if 'humidity' in content:
                value = content['humidity']
                source_name = f'{node_name}-rh'
                updated |= update_db(int(time.time()), source_name, value)
            if 'pressure' in content:
                value = content['pressure']
                source_name = f'{node_name}-p'
                updated |= update_db(int(time.time()), source_name, value)
            if 'linkquality' in content:
                value = content['linkquality']
                source_name = f'{node_name}-link'
                updated |= update_db(int(time.time()), source_name, value)
            if 'voltage' in content:
                value = content['voltage']
                source_name = f'{node_name}-v'
                updated |= update_db(int(time.time()), source_name, value)
            if updated:
                db.commit()
        except Exception as e:
            log.warning(f'Bad payload: {msg.topic}, {msg.payload}: {e}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('mqtt_to_sql')

    parser = argparse.ArgumentParser(description="Pull temperature readings from MQTT and add to a SQL database")
    parser.add_argument("--mqtt", metavar="ADDRESS", default="localhost", help="MQTT broker address")
    parser.add_argument("--db", metavar="FILE", default="samples.sqlite", help="SQLite database file")
    parser.add_argument("--debug", default=False, action="store_true", help="Enable debug printouts")
    parser_sec = parser.add_argument_group("Security")
    parser_sec.add_argument("--tls-insecure", action="store_true", default=False,
                        help="Disable hostname verification against cert")
    parser_sec.add_argument("--tls-ca", help="CA certificate that has signed the server's certificate")
    parser_sec.add_argument("--username", "-u", help="Username")
    parser_sec.add_argument("--password", "-p", help="Password")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    client_id = f'mqtt_to_sql-{platform.node()}'
    db = sqlite3.connect(args.db)
    db.execute('CREATE TABLE IF NOT EXISTS series (id INTEGER PRIMARY KEY, name TEXT UNIQUE);')
    db.execute('CREATE TABLE IF NOT EXISTS samples '
               '(time INTEGER, series INTEGER, value REAL, '
               'FOREIGN KEY(series) REFERENCES series(id));')
    db.commit()

    client = mqtt.Client(client_id=client_id, clean_session=True)
    port = 1883
    if args.tls_ca is not None:
        client.tls_set(ca_certs=args.tls_ca)
        client.tls_insecure_set(args.tls_insecure)
        port = 8883
    client.on_connect = on_connect
    client.on_message = on_message
    if args.username:
        client.username_pw_set(args.username, args.password)
    client.connect(args.mqtt, port=port)
    log.info(f'Connected as {client_id}')
    
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
