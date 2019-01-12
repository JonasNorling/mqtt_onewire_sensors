#!/usr/bin/env python3
#
# Pull temperature readings from MQTT and add to an RRD
#

import argparse
import logging
import platform
import paho.mqtt.client as mqtt
import time
import re
import subprocess
import sys
from pathlib import Path

TOPIC_MATCH = "temperature/#"
TOPIC_RE = re.compile(TOPIC_MATCH.replace("#", "(.+)"))

rrd_path = None

def create_rrd(rrdfile, prefill_src=None, prefill_ds=None):
    try:
        log.info("Creating RRD %s" % rrdfile)
        prefill_opts = []
        prefill_ds_exp = ""
        if prefill_src is not None:
            prefill_opts = ["--source", prefill_src]
            prefill_ds_exp = "=" + prefill_ds
        # 4 weeks with minute level data
        HIGH_RES_SAMPLES = 40320
        # 365 days with ten minute level data
        MED_RES_SAMPLES = 52560
        # ten years with hour level data
        LOW_RES_SAMPLES = 87600
        completed = subprocess.run(["rrdtool", "create", str(rrdfile),
                *prefill_opts,
                "-O", "--step", "60",
                "DS:value%s:GAUGE:120:-60:60" % prefill_ds_exp,
                "RRA:AVERAGE:0.5:1:%d" % HIGH_RES_SAMPLES,
                "RRA:AVERAGE:0.5:10:%d" % MED_RES_SAMPLES,
                "RRA:AVERAGE:0.5:60:%d" % LOW_RES_SAMPLES,
                "RRA:MAX:0.5:1:%d" % HIGH_RES_SAMPLES,
                "RRA:MAX:0.5:10:%d" % MED_RES_SAMPLES,
                "RRA:MAX:0.5:60:%d" % LOW_RES_SAMPLES,
                "RRA:MIN:0.5:1:%d" % HIGH_RES_SAMPLES,
                "RRA:MIN:0.5:10:%d" % MED_RES_SAMPLES,
                "RRA:MIN:0.5:60:%d" % LOW_RES_SAMPLES])
        if completed.returncode != 0:
            log.error("RRD create failed")
    except FileNotFoundError as e:
        log.error(e)

def update_rrd(timestamp, sensorname, value):
    log.debug("Reading at %d for %s: %.3f" % (timestamp, sensorname, value))
    rrdfile = Path(rrd_path, "%s.rrd" % sensorname)
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
    client.subscribe(TOPIC_MATCH)

def on_message(client, userdata, msg):
    log.debug("Message: %s %s" % (msg.topic, msg.payload))
    match = TOPIC_RE.match(msg.topic)
    if match:
        try:
            sensorname = match.group(1)
            value = float(msg.payload)
        except ValueError:
            log.warning("Bad payload: %s %s"% (msg.topic, msg.payload))
            return

        update_rrd(int(time.time()), sensorname, value)

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
