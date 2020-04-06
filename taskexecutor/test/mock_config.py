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
mock_config.CONFIG.builtinservice.sysconf_dir = '/nowhere/etc'
mock_config.CONFIG.builtinservice.cgroupfs_mountpoint = '/sys/fs/cgroup'
mock_config.CONFIG.builtinservice.linux_user_manager.default_shell = '/bin/bash'
mock_config.CONFIG.builtinservice.linux_user_manager.disabled_shell = '/bin/false'
mock_config.CONFIG.builtinservice.linux_user_manager.min_uid = 2000
mock_config.CONFIG.builtinservice.linux_user_manager.limitgroup = 'limitgroup'
