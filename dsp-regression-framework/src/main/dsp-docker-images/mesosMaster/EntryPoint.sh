#!/bin/bash

export MESOS_SYSTEMD_ENABLE_SUPPORT=false
echo exit 0 > /usr/sbin/policy-rc.d
mesos-master --port=5050  --cluster=mesos-master --work_dir=/etc/wdir/ --advertise_ip=${HOST_MACHINE_IP} --hostname=Mesos-Master --quorum=1 --log_dir=/etc/logs > /etc/logs/master.logs 2>&1 &
tail -f /dev/null