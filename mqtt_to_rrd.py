#!/usr/bin/env python3
#
# Pull temperature readings from MQTT and add to an RRD
#

import argparse
import logging
import platform
from collections import namedtuple

import paho.mqtt.client as mqtt
import time
import re
import subprocess
import sys
from pathlib import Path
import json
from contextlib import suppress
from typing import Dict, List

topic_data = namedtuple('topic_data', 'mqtt,re,handler')
rrd_path = None
last_samples: Dict[str, int] = {}


def create_rrd(rrdfile, prefill_src=None, prefill_ds=None):
    try:
        log.info("Creating RRD %s" % rrdfile)
        prefill_opts = []
        prefill_ds_exp = ""
        if prefill_src is not None:
            prefill_opts = ["--source", prefill_src]
            prefill_ds_exp = "=" + prefill_ds
        # 1 year with minute level data
        HIGH_RES_SAMPLES = 365*24*60
        # ten years with hour level data
        LOW_RES_SAMPLES = 10*365*24
        completed = subprocess.run(["rrdtool", "create", str(rrdfile),
                *prefill_opts,
                "-O", "--step", "60",
                "DS:value%s:GAUGE:4000:-100:10000" % prefill_ds_exp,
                "RRA:AVERAGE:0.5:1:%d" % HIGH_RES_SAMPLES,
                "RRA:AVERAGE:0.5:60:%d" % LOW_RES_SAMPLES,
                "RRA:MAX:0.5:1:%d" % HIGH_RES_SAMPLES,
                "RRA:MAX:0.5:60:%d" % LOW_RES_SAMPLES,
                "RRA:MIN:0.5:1:%d" % HIGH_RES_SAMPLES,
                "RRA:MIN:0.5:60:%d" % LOW_RES_SAMPLES])
        if completed.returncode != 0:
            log.error("RRD create failed")
    except FileNotFoundError as e:
        log.error(e)


def update_rrd(timestamp, source_name, value):
    log.debug("Reading at %d for %s: %.3f" % (timestamp, source_name, value))
    with suppress(KeyError):
        if last_samples[source_name] == timestamp:
            log.debug("Suppressing duplicate reading")
            return
    last_samples[source_name] = timestamp

    rrdfile = Path(rrd_path, "%s.rrd" % source_name)
    if not rrdfile.is_file():
        create_rrd(str(rrdfile))
    try:
        completed = subprocess.run(["rrdtool", "update", str(rrdfile),
                "%d:%f" % (timestamp, value)])
        if completed.returncode != 0:
            log.error("RRD update failed")
    except FileNotFoundError as e:
        log.error(e)


def on_connect(client, userdata, flags, rc):
    log.info("Connected: %s" % rc)

    client.subscribe("$SYS/broker/version")
    for topic in topics:
        client.subscribe(topic.mqtt)


def on_message(client, userdata, msg):
    log.debug("Message: %s %s" % (msg.topic, msg.payload))
    for topic in topics:
        match = topic.re.match(msg.topic)
        if match:
            try:
                topic.handler(match[0], msg.payload)
            except Exception as e:
                log.warning(f'Bad payload: {msg.topic}, {msg.payload}: {e}')


def handle_json_topic(node_name, payload):
    content = json.loads(payload)
    if 'temperature' in content:
        value = content['temperature']
        source_name = f'{node_name}-t'
        update_rrd(int(time.time()), source_name, value)
    if 'humidity' in content:
        value = content['humidity']
        source_name = f'{node_name}-rh'
        update_rrd(int(time.time()), source_name, value)
    if 'linkquality' in content:
        value = content['linkquality']
        source_name = f'{node_name}-link'
        update_rrd(int(time.time()), source_name, value)
    if 'voltage' in content:
        value = content['voltage']
        source_name = f'{node_name}-v'
        update_rrd(int(time.time()), source_name, value)


def handle_float_topic(node_name, payload):
    value = float(payload)
    update_rrd(int(time.time()), node_name, value)


topics: List[topic_data] = [
    topic_data('zigbee2mqtt/+', re.compile(r'zigbee2mqtt/(.*)'), handle_json_topic),
    topic_data('temperature/+', re.compile(r'temperature/(.*)'), handle_float_topic),
]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("mqtt_to_rrd")

    parser = argparse.ArgumentParser(description="Pull temperature readings from MQTT and add to an RRD")
    parser.add_argument("--mqtt", metavar="ADDRESS", default="localhost",
                        help="MQTT broker address")
    parser.add_argument("--rrd-path", metavar="PATH", default=".",
                        help="Path to directory with RRD files")
    parser.add_argument("--debug", default=False, action="store_true",
                        help="Enable debug printouts")
    parser.add_argument("--prefill", nargs=3, metavar=("NEW-RRD", "OLD-RRD", "DS"),
                        help="Create a prefilled RRD")
    parser_sec = parser.add_argument_group("Security")
    parser_sec.add_argument("--tls-insecure", action="store_true", default=False,
                        help="Disable hostname verification against cert")
    parser_sec.add_argument("--tls-ca", help="CA certificate that has signed the server's certificate")
    parser_sec.add_argument("--username", "-u", help="Username")
    parser_sec.add_argument("--password", "-p", help="Password")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    if args.prefill:
        create_rrd(*args.prefill)
        sys.exit(0)

    client_id = "%s-%s" % ("mqtt_to_rrd", platform.node())
    rrd_path = args.rrd_path

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
    log.info("Connected as %s" % client_id)
    
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
