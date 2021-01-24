#!/bin/bash

echo exit 0 > /usr/sbin/policy-rc.d
export REPO_SERVICE_URL=0.0.0.0
export REPO_SERVICE_PORT=8080
export REPO_SERVICE_ENV=dsp-mesos
export REPO_SERVICE_ENV_VERSION=17
export REPO_SERVICE_APP_KEY=clientkey

export DOCKER_USER=fk-idf-dev

echo "deb http://0.0.0.0/repos/infra-cli/3 /" > /etc/apt/sources.list.d/infra-cli.list
apt-get update
apt-get install --yes --allow-unauthenticated infra-cli

reposervice --host $REPO_SERVICE_URL --port $REPO_SERVICE_PORT getenv --name $REPO_SERVICE_ENV --appkey $REPO_SERVICE_APP_KEY --version $REPO_SERVICE_ENV_VERSION > /etc/apt/sources.list.d/mesos-executor.list
bash -c "echo 'deb [trusted=yes] http://0.0.0.0/repos/fk-ops-servicebuilder/3 /' >> /etc/apt/sources.list.d/zk.list"
apt-get update

apt-get install --yes --allow-unauthenticated fk-config-service-confd
apt-get install --yes --allow-unauthenticated fk-ops-servicebuilder
apt-get install --yes --allow-unauthenticated fk-ops-hosts-populator
apt-get install --yes --allow-unauthenticated oracle-j2sdk1.8
apt-get install --yes --allow-unauthenticated mesos
apt-get install --yes --allow-unauthenticated cron
apt-get install --yes --allow-unauthenticated procps
mkdir /etc/logs/
mkdir /etc/wdir/