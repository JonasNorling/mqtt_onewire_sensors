import logging
import paho.mqtt.client as mqtt


class MqttConnection:
    def __init__(self, client_id: str, args, log=logging.getLogger()):
        self.log = log.getChild('mqtt-connection')
        self.client = mqtt.Client(client_id=client_id)
        self.client_id = client_id
        port = 1883
        if args.tls_ca is not None:
            self.client.tls_set(ca_certs=args.tls_ca)
            self.client.tls_insecure_set(args.tls_insecure)
            port = 8883
        self.client.on_connect = self.on_connect
        if args.username:
            self.client.username_pw_set(args.username, args.password)

        self.state_topic = f'state/{self.client_id}'
        self.client.will_set(self.state_topic, 'crashed', retain=True)
        self.client.connect(args.mqtt, port=port)

    def start(self):
        self.client.loop_start()

    def stop(self):
        self.client.publish(self.state_topic, 'disconnected')
        self.client.loop_stop()
        self.client.disconnect()

    def publish(self, topic: str, data: str):
        self.client.publish(topic, data)

    def on_connect(self, mqtt_client, userdata, flags, rc):
        if rc == 0:
            self.log.info(f'Connected')
            self.client.publish(self.state_topic, 'connected', retain=True)
        else:
            self.log.warning(f'Connection failed: {mqtt.connack_string(rc)}')

    @staticmethod
    def add_args(parser):
        parser_mqtt = parser.add_argument_group("MQTT")
        parser_mqtt.add_argument("--mqtt", metavar="ADDRESS", default="localhost",
                                 help="MQTT broker address")
        parser_mqtt.add_argument("--tls-insecure", action="store_true", default=False,
                                 help="Disable hostname verification against cert")
        parser_mqtt.add_argument("--tls-ca", help="CA certificate that has signed the server's certificate")
        parser_mqtt.add_argument("--username", "-u", help="Username")
        parser_mqtt.add_argument("--password", "-p", help="Password")
