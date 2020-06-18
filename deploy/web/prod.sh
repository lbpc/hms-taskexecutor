#!/usr/bin/env bash

set -eux
set -o pipefail

docker pull docker-registry.intr/hms/taskexecutor:master

cat > /etc/init/taskexecutor.conf <<EOF
start on filesystem and started docker
stop on runlevel [!2345]
respawn
script
/usr/bin/docker run                                                                     \
  --hostname="$(hostname -s)"                                                           \
  --cap-add SYS_ADMIN                                                                   \
  --device "$(mount | grep home | grep dev | awk '{print $1}')"                         \
  --init                                                                                \
  --mount source=/etc,target=/etc,type=bind                                             \
  --mount source=/home,target=/home,type=bind                                           \
  --mount source=/opcache,target=/opcache,type=bind                                     \
  --mount source=/opt,target=/opt,type=bind                                             \
  --mount source=/root/.cache,target=/root/.cache,type=bind                             \
  --mount source=/run/docker.sock,target=/var/run/docker.sock,type=bind                 \
  --mount source=/sys/fs/cgroup,target=/sys/fs/cgroup,type=bind                         \
  --mount source=/var/cache,target=/var/cache,type=bind                                 \
  --mount source=/var/lib/docker/containers,target=/var/lib/docker/containers,type=bind \
  --mount source=/var/log,target=/var/log,type=bind                                     \
  --mount target=/tmp,type=tmpfs                                                        \
  --mount target=/var/tmp,type=tmpfs                                                    \
  --network=host                                                                        \
  --pid=host                                                                            \
  --privileged                                                                          \
  --read-only                                                                           \
  --log-driver=json-file                                                                \
  --name=taskexecutor                                                                   \
  --restart=always                                                                      \
  -e APIGW_HOST=api.intr                                                                \
  -e APIGW_PASSWORD=Efu0ahs6                                                            \
  -e CONFIG_PROFILE=prod                                                                \
  -e TE_AMQP_HOST=rabbit.intr                                                           \
  -e TE_MAX-WORKERS_BACKUP_FILES=4                                                      \
  -e TE_SCHEDULE_BACKUP_UNIX-ACCOUNT_AT=02:11                                           \
  -e TE_SCHEDULE_BACKUP_UNIX-ACCOUNT_DAILY=True                                         \
  -e TE_SCHEDULE_BACKUP_UNIX-ACCOUNT_EXEC-TYPE=parallel                                 \
  -e TE_TASK_MAX-RETRIES=5                                                              \
  docker-registry.intr/hms/taskexecutor:master_refactored
end script
post-stop script
  /usr/bin/docker rm -f taskexecutor
end script
EOF
