#!/bin/bash

echo exit 0 > /usr/sbin/policy-rc.d

export DOCKER_CHANNEL=stable

apt-get update && apt-get install -y --no-install-recommends \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg2 \
  software-properties-common

curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -

add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/debian \
   $(lsb_release -cs) \
   ${DOCKER_CHANNEL}"


apt-get update && apt-cache policy docker-ce && apt-get install -y --no-install-recommends docker-ce && \
  docker -v && \
  dockerd -v
