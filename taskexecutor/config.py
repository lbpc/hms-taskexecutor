import yaml
import socket

with open("config.yml", 'r') as yml:
	CONFIG = yaml.load(yml)
hostname = socket.gethostname().split('.')[0]
CONFIG["amqp"]["consumer_routing_key"] = "te@{}".format(hostname)
