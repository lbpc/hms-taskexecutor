from unittest.mock import Mock

mock_config = Mock()

mock_config.CONFIG.amqp.user = 'guest'
mock_config.CONFIG.amqp.password = 'guest'
mock_config.CONFIG.amqp.host = '127.0.0.1'
mock_config.CONFIG.amqp.port = 5672
mock_config.CONFIG.amqp.exchange_type = 'topic'
mock_config.CONFIG.amqp.consumer_routing_key = 'te.web99'
mock_config.CONFIG.amqp.connection_attempts = 1
mock_config.CONFIG.amqp.retry_delay = 5
mock_config.CONFIG.amqp.heartbeat_interval = 30
mock_config.CONFIG.amqp.connection_timeout = 5
mock_config.CONFIG.enabled_resources = ['unix-account', 'database-user', 'database', 'website', 'ssl-certificate']
mock_config.CONFIG.conffile.tmp_dir = '/nowhere/conf'
mock_config.CONFIG.conffile.bad_confs_dir = '/nowhere/conf-broken'

