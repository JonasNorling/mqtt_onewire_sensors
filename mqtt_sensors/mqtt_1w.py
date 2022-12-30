#!/usr/bin/env python3
#
# Publish one-wire sensor values with MQTT
#

import argparse
import logging
import platform
from pathlib import Path
import time

from .mqtt_connection import MqttConnection

TOPIC = "temperature/%s"
W1_PATH = "/sys/bus/w1/devices/"

log = logging.getLogger("mqtt_1w")


def sample_loop(sample_interval, mqtt_connection: MqttConnection):
    last_time = time.monotonic() - sample_interval
    while True:
        try:
            time.sleep(last_time + sample_interval - time.monotonic())
        except ValueError:
            pass

        last_time = time.monotonic()
        sample_onewire(mqtt_connection)


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


def sample_onewire(mqtt_connection: MqttConnection):
    log.debug("Sampling one-wire sensors")

    for s_path in Path(W1_PATH).glob("*/w1_slave"):
        sensor_name = s_path.parts[-2]
        log.debug("Reading sensor %s" % sensor_name)
        try:
            text = s_path.read_text()
            temperature = parse(text.splitlines())
            log.debug("Sensor %s = %.3f C" % (sensor_name, temperature))
            mqtt_connection.publish(TOPIC % sensor_name, str(temperature))
        except (IOError, RuntimeError) as e:
            log.warning(e)


def run():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Publish one-wire sensor values with MQTT")
    parser.add_argument("--time", metavar="SEC", default=20, type=int,
                        help="Sample interval [seconds]")
    parser.add_argument("--debug", default=False, action="store_true",
                        help="Enable debug printouts")
    MqttConnection.add_args(parser)
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    mqtt_connection = MqttConnection(f'1w-{platform.node()}', args, log)

    try:
        mqtt_connection.start()
        sample_loop(args.time, mqtt_connection)
    except KeyboardInterrupt:
        log.info('Exit on CTRL-C')
    finally:
        mqtt_connection.stop()


if __name__ == "__main__":
    run()
