```root@web99:~# docker image inspect docker-registry.intr/hms/taskexecutor:latest | jq .[].Config.Labels
{
  "ru.majordomo.docker.cmd": "docker run --init --read-only --pid=host --network=host --hostname=$(hostname -s) --mount source=/run/docker.sock,target=/var/run/docker.sock,type=bind --mount source=/home,target=/home,type=bind --mount source=/opt,target=/opt,type=bind --mount source=/var/cache,target=/var/cache,type=bind --mount source=/etc,target=/etc,type=bind --mount target=/var/tmp,type=tmpfs docker-registry.intr/hms/taskexecutor:latest"
}
root@web99:~# eval $(docker image inspect docker-registry.intr/hms/taskexecutor:latest | jq -r '.[].Config.Labels["ru.majordomo.docker.cmd"]')
2020-04-24 12:12:05.030 INFO --- (config:37 _fetch_remote_properties) MainThread        : Fetching properties from config server
2020-04-24 12:12:05.588 INFO --- (config:74 _fetch_remote_properties) MainThread        : Server roles: ['shared-hosting', 'mysql-database-server'], manageable resources: ['service', 'unix-account', 'website', 'ssl-certificate', 'resource-archive', 'redirect', 'service', 'database-user', 'database', 'resource-archive']
2020-04-24 12:12:05.808 INFO --- (__main__:54 <module>) MainThread      : Executor thread started
...

root@web99:~# eval $(echo $(docker image inspect docker-registry.intr/hms/taskexecutor:latest | jq -r '.[].Config.Labels["ru.majordomo.docker.cmd"]') | awk 'NF{$3="-it" OFS $3} 1') code
Python 3.7.4 (default, Jul  8 2019, 18:31:06) 
[GCC 8.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
(InteractiveConsole)
>>> from taskexecutor.constructor import get_http_proxy_service
2020-04-24 12:23:14.594 INFO --- (config:37 _fetch_remote_properties) MainThread        : Fetching properties from config server
2020-04-24 12:23:15.045 INFO --- (config:74 _fetch_remote_properties) MainThread        : Server roles: ['shared-hosting', 'mysql-database-server'], manageable resources: ['service', 'unix-account', 'website', 'ssl-certificate', 'resource-archive', 'redirect', 'service', 'database-user', 'database', 'resource-archive']
>>> ngx = get_http_proxy_service()
>>> ngx.status()
<ServiceStatus.UP: True>
>>> ngx.stop()
2020-04-24 12:23:41.651 INFO --- (opservice:480 stop) MainThread        : Stopping and removing container nginx
>>> ngx.reload()
2020-04-24 12:23:49.423 WARNING --- (opservice:511 reload) MainThread   : nginx is down, starting it
2020-04-24 12:23:49.424 INFO --- (opservice:422 _pull_image) MainThread : Pulling docker-registry.intr/webservices/nginx:master docker image
2020-04-24 12:23:49.487 INFO --- (opservice:463 start) MainThread       : Docker image docker-registry.intr/webservices/nginx:master has run arguments hints: {'init': True, 'network': 'host', 'read_only': True, 'volumes': [{'read_only': True, 'source': '/opt/nginx/conf', 'target': '/read/conf', 'type': 'bind'}, {'read_only': True, 'source': '/etc/nginx/ssl.key', 'target': '/read/ssl', 'type': 'bind'}, {'read_only': True, 'source': '/etc/nginx/sites-available', 'target': '/read/sites', 'type': 'bind'}, {'source': '/home/nginx', 'target': '/write/cache', 'type': 'bind'}, {'read_only': True, 'source': '/home', 'target': '/home', 'type': 'bind'}, {'target': '/var/run', 'type': 'tmpfs'}, {'target': '/var/spool/nginx', 'type': 'tmpfs'}]}
2020-04-24 12:23:49.537 INFO --- (opservice:422 _pull_image) MainThread : Pulling docker-registry.intr/webservices/nginx:master docker image
2020-04-24 12:24:23.334 INFO --- (opservice:471 start) MainThread       : Creating /opt/nginx/conf directory
2020-04-24 12:24:23.334 INFO --- (opservice:471 start) MainThread       : Creating /etc/nginx/ssl.key directory
2020-04-24 12:24:23.334 INFO --- (opservice:471 start) MainThread       : Creating /etc/nginx/sites-available directory
2020-04-24 12:24:23.334 INFO --- (opservice:471 start) MainThread       : Creating /home/nginx directory
2020-04-24 12:24:23.334 INFO --- (opservice:471 start) MainThread       : Creating /home directory
2020-04-24 12:24:23.338 INFO --- (opservice:476 start) MainThread       : Running container nginx with arguments: {'name': 'nginx', 'detach': True, 'init': True, 'tty': False, 'restart_policy': {'Name': 'always'}, 'network': 'host', 'read_only': True, 'mounts': [{'Target': '/read/conf', 'Source': '/opt/nginx/conf', 'Type': 'bind', 'ReadOnly': True}, {'Target': '/read/ssl', 'Source': '/etc/nginx/ssl.key', 'Type': 'bind', 'ReadOnly': True}, {'Target': '/read/sites', 'Source': '/etc/nginx/sites-available', 'Type': 'bind', 'ReadOnly': True}, {'Target': '/write/cache', 'Source': '/home/nginx', 'Type': 'bind', 'ReadOnly': False}, {'Target': '/home', 'Source': '/home', 'Type': 'bind', 'ReadOnly': True}, {'Target': '/var/run', 'Source': None, 'Type': 'tmpfs', 'ReadOnly': False}, {'Target': '/var/spool/nginx', 'Source': None, 'Type': 'tmpfs', 'ReadOnly': False}], 'ports': {}}
>>> 
now exiting InteractiveConsole...
```

