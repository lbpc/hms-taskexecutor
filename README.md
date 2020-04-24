```root@web99:~# docker image inspect
docker-registry.intr/hms/taskexecutor:latest | jq .[].Config.Labels
{
  "ru.majordomo.docker.cmd": "docker run --init --read-only --pid=host
--network=host --hostname=$(hostname -s) --mount
source=/run/docker.sock,target=/var/run/docker.sock,type=bind --mount
source=/home,target=/home,type=bind --mount source=/opt,target=/opt,type=bind
--mount source=/var/cache,target=/var/cache,type=bind --mount
source=/etc,target=/etc,type=bind --mount target=/var/tmp,type=tmpfs
docker-registry.intr/hms/taskexecutor:latest"
}
root@web99:~# eval $(docker image inspect
docker-registry.intr/hms/taskexecutor:latest | jq -r
'.[].Config.Labels["ru.majordomo.docker.cmd"]')
2020-04-24 12:12:05.030 INFO --- (config:37 _fetch_remote_properties) MainThread
: Fetching properties from config server
2020-04-24 12:12:05.588 INFO --- (config:74 _fetch_remote_properties) MainThread
: Server roles: ['shared-hosting', 'mysql-database-server'], manageable
resources: ['service', 'unix-account', 'website', 'ssl-certificate',
'resource-archive', 'redirect', 'service', 'database-user', 'database',
'resource-archive']
2020-04-24 12:12:05.808 INFO --- (__main__:54 <module>) MainThread      :
Executor thread started
...

root@web99:~# eval $(echo $(docker image inspect
docker-registry.intr/hms/taskexecutor:latest | jq -r
'.[].Config.Labels["ru.majordomo.docker.cmd"]') | awk 'NF{$3="-it" OFS $3} 1')
code
Python 3.7.4 (default, Jul  8 2019, 18:31:06) 
[GCC 8.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
(InteractiveConsole)
>>> from taskexecutor.constructor import get_http_proxy_service
2020-04-24 12:39:20.519 INFO --- (config:37 _fetch_remote_properties) MainThread
: Fetching properties from config server
2020-04-24 12:39:20.982 INFO --- (config:74 _fetch_remote_properties) MainThread
: Server roles: ['shared-hosting', 'mysql-database-server'], manageable
resources: ['service', 'unix-account', 'website', 'ssl-certificate',
'resource-archive', 'redirect', 'service', 'database-user', 'database',
'resource-archive']
>>> from taskexecutor.logger import LOGGER
>>> LOGGER.setLevel('WARNING')
>>> ngx = get_http_proxy_service()
>>> ngx.status()
<ServiceStatus.UP: True>
>>> ngx.stop()
>>> ngx.status()
<ServiceStatus.DOWN: False>
>>> ngx.reload()
2020-04-24 12:40:11.414 WARNING --- (opservice:511 reload) MainThread   : nginx
is down, starting it
>>> ngx.status()
<ServiceStatus.UP: True>
>>> 
now exiting InteractiveConsole...
```
