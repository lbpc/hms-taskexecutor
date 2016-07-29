import yaml
import socket

with open("config.yml", 'r') as yml:
	CONFIG = yaml.load(yml)
HOSTNAME = socket.gethostname().split('.')[0]
CONFIG["amqp"]["consumer_routing_key"] = "TE@{}".format(HOSTNAME)