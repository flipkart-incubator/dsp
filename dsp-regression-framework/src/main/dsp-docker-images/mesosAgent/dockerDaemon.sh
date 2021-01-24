#!/bin/bash

mkdir /etc/docker/
touch /etc/docker/daemon.json
echo "{ \"insecure-registries\":[\"0.0.0.0:5000\",\"0.0.0.0\"] }" > /etc/docker/daemon.json;
