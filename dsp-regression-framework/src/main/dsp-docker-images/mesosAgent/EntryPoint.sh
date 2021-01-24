#!/bin/bash

export MESOS_SYSTEMD_ENABLE_SUPPORT=false
echo exit 0 > /usr/sbin/policy-rc.d
service docker start
service docker status
docker info

/usr/sbin/mesos-slave --resources='cpus:8;mem:50000;' --work_dir=/etc/wdir/ --advertise_ip=${HOST_MACHINE_IP} --containerizers=docker,mesos --hostname=${HOST_MACHINE_IP} --isolation=cgroups/cpu,cgroups/mem  --master=${HOST_MACHINE_IP}:5050 --port=5051 --log_dir=/etc/logs > /etc/logs/slave.logs 2>&1 &

mkdir /etc/logs/
chmod 777 /etc/logs/
mkdir /etc/wdir/
chmod 777 /etc/wdir/

REPO_NAME="${EXECUTION_ENVIRONMENT}-ipp-dsp-workflow-executor"
sed -i '/'"$REPO_NAME"'/c\deb [trusted=yes] http://0.0.0.0/repos/'"$REPO_NAME"'/'"$MESOS_AGENT_DEBIAN_VERSION"' /' /etc/apt/sources.list.d/mesos-executor.list
rm -r /etc/default/cosmos-role

echo "Regression${HOST_MACHINE_IP}" >>  /etc/default/cosmos-role

rm /etc/apt/sources.list
echo "deb http://0.0.0.0/debian jessie main" > /etc/apt/sources.list
echo "deb http://0.0.0.0/debian-security/ jessie/updates main" >> /etc/apt/sources.list

apt-get update
apt-get install --yes --allow-unauthenticated ${EXECUTION_ENVIRONMENT}-ipp-dsp-workflow-executor-${MESOS_AGENT_PACKAGE_VERSION}
cat  /etc/temp_t >> /etc/hosts

mkdir -p /etc/authn/regression
cp /etc/authn/$EXECUTION_ENVIRONMENT/* /etc/authn/regression

if [[ "$EXECUTION_ENVIRONMENT" != "prod" ]]; then
  mkdir -p /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION/bin/prod
  mkdir -p /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION/lib/prod
  mkdir -p /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION/image_details/prod
  cp /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION/bin/$EXECUTION_ENVIRONMENT/* /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION/bin/prod
  cp /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION/lib/$EXECUTION_ENVIRONMENT/* /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION/lib/prod
  cp /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION/image_details/$EXECUTION_ENVIRONMENT/* /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION/image_details/prod
  ls -lh /usr/share/ipp-dsp-workflow-executor/$MESOS_AGENT_PACKAGE_VERSION
fi

echo '*/2 * * * * root docker ps --filter status=exited -aq | xargs -r docker rm -v >> /tmp/docker-exited-cleanup.log' | tee /etc/cron.daily/docker-cleanup-exited
tail -f /dev/null