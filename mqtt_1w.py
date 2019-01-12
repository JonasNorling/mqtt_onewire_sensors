#!/usr/bin/env python3
#
# Publish one-wire sensor values with MQTT
#

import argparse
import logging
import platform
import paho.mqtt.client as mqtt
from pathlib import Path
import time

TOPIC = "temperature/%s"
W1_PATH = "/sys/bus/w1/devices/"

def on_connect(mqtt_client, userdata, flags, rc):
    log.info("Connected: %s" % rc)

def sample_loop(t, mqtt_client):
    last_time = time.time() - t
    while True:
        t = args.time
        try:
            time.sleep(last_time + t - time.time())
        except ValueError:
            pass

        last_time = time.time()
        sample_onewire(mqtt_client)

def parse(lines):
    if lines[0].strip()[-3:] != 'YES':
        raise RuntimeError("Bad CRC")
    equals_pos = lines[1].find('t=')
    if equals_pos != -1:
        temp_string = lines[1][equals_pos+2:]
        t = float(temp_string) / 1000.0
        if t == 85.000:
            raise RuntimeError("Missing temperature reading")
        return t
    else:
        raise RuntimeError("Unable to parse")

def sample_onewire(mqtt_client):
    log.debug("Sampling one-wire sensors")

    for s_path in Path(W1_PATH).glob("*/w1_slave"):
        sensor_name = s_path.parts[-2]
        log.debug("Reading sensor %s" % sensor_name)
        try:
            text = s_path.read_text()
            temperature = parse(text.splitlines())
            log.debug("Sensor %s = %.3f C" % (sensor_name, temperature))
            mqtt_client.publish(TOPIC % sensor_name, temperature)
        except (IOError, RuntimeError) as e:
            log.warning(e)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("mqtt_1w")

    parser = argparse.ArgumentParser(description="Publish one-wire sensor values with MQTT")
    parser.add_argument("--mqtt", metavar="ADDRESS", default="localhost",
                        help="MQTT broker address")
    parser.add_argument("--time", metavar="SEC", default=20, type=int,
                        help="Sample interval [seconds]")
    parser.add_argument("--debug", default=False, action="store_true",
                        help="Enable debug printouts")
    parser_sec = parser.add_argument_group("Security")
    parser_sec.add_argument("--tls-insecure", action="store_true", default=False,
                        help="Disable hostname verification against cert")
    parser_sec.add_argument("--tls-ca", help="CA certificate that has signed the server's certificate")
    parser_sec.add_argument("--username", "-u", help="Username")
    parser_sec.add_argument("--password", "-p", help="Password")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    client_id = "%s-%s" % ("1w", platform.node())
    client = mqtt.Client(client_id=client_id)
    port = 1883
    if args.tls_ca is not None:
        client.tls_set(ca_certs=args.tls_ca)
        client.tls_insecure_set(args.tls_insecure)
        port = 8883
    client.on_connect = on_connect
    if args.username:
        client.username_pw_set(args.username, args.password)
    client.connect(args.mqtt, port=port)

    try:
        client.loop_start()
        sample_loop(args.time, client)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()
