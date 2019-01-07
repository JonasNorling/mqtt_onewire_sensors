## mqtt_1w.py: Publish one-wire sensor values with MQTT

This script is intended to be run on a Raspberry Pi to publish temperature sensor readings to MQTT. It should be portable to any platform where /sys/bus/w1 is available (CONFIG_W1 and CONFIG_W1_MASTER_GPIO).

Dependencies:

- sudo pip3 install paho-mqtt

### Running on Raspberry Pi

One-wire must be enabled on the Raspberry Pi, either by enabling the appropriate option with `raspi-config` or by adding `dtoverlay=w1-gpio` to `/boot/config.txt`. When properly enabled, any one-wire devices should appear as `/sys/bus/w1/devices/*/w1_slave`.

See https://pinout.xyz/pinout/1_wire.

This script can be run as a service from systemd. See mqtt_1w.service for installation instructions.


## mqtt_to_rrd.py: Save sensor values to RRD from MQTT

Dependencies:

- sudo apt-get install rrdtool
