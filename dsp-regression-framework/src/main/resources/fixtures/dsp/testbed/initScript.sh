#!/bin/sh
export HOST_MACHINE_IP=$(hostname --ip-address)
echo $HOST_MACHINE_IP  regression-host >> /etc/hosts