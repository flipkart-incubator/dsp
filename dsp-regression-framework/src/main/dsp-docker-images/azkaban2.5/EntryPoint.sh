#!/bin/bash

cat  /etc/temp_t >> /etc/hosts
sudo apt-get install --yes --allow-unauthenticated   hadoop-infra-hadoop-conf

echo "Update toml and tmpl files"
mv /etc/confd/conf.d/hadoopcluster2-etl-azkaban-web-server.toml /etc/confd/conf.d/dsp-azkaban-web-server.toml
mv /etc/confd/conf.d/hadoopcluster2-etl-azkaban-exec-server.toml /etc/confd/conf.d/dsp-azkaban-exec-server.toml
mv /etc/confd/templates/hadoopcluster2-etl-azkaban-exec-server.tmpl /etc/confd/templates/dsp-azkaban-exec-server.tmpl
mv /etc/confd/templates/hadoopcluster2-etl-azkaban-web-server.tmpl /etc/confd/templates/dsp-azkaban-web-server.tmpl

apt-get install --yes --allow-unauthenticated stream-relay
sed -i '/0.0.0.0/c\mysql.host='"$HOST_MACHINE_IP"'' /app/azkaban_web.properties
sed -i '/azkaban.name=Ragnarok/c\azkaban.name=DSP_Regression' /app/azkaban_web.properties
sed -i '/azkaban.label=Ragnarok/c\azkaban.label=DSP_Regression' /app/azkaban_web.properties
sed -i '/0.0.0.0/c\mysql.host='"$HOST_MACHINE_IP"'' /app/azkaban_exec.properties

cp /app/azkaban_web.properties /usr/share/fk-idf-azkaban/azkaban-web-server/conf/azkaban.properties
cp /app/azkaban_exec.properties /usr/share/fk-idf-azkaban/azkaban-exec-server/conf/azkaban.properties

/etc/init.d/ip-azkaban-web-start.sh
/etc/init.d/ip-azkaban-executor-start.sh
sidekick &

REPO_NAME="${EXECUTION_ENVIRONMENT}-ipp-dsp-azkaban-repo"
if [ -f "/etc/apt/sources.list.d/dsp-azkaban.list" ]; then
  sed -i '/'"$REPO_NAME"'/c\deb [trusted=yes] http://0.0.0.0/repos/'"$REPO_NAME"'/'"$REGRESSION_AZKABAN_DEBIAN_VERSION"' /' /etc/apt/sources.list.d/dsp-azkaban.list
else
  echo "deb [trusted=yes] http://0.0.0.0/repos/"$REPO_NAME"/"$REGRESSION_AZKABAN_DEBIAN_VERSION" /" > /etc/apt/sources.list.d/dsp-azkaban.list
fi

apt-get update
apt-get download ipp-dsp-azkaban-"$EXECUTION_ENVIRONMENT"-"$AZKABAN_PACKAGE_VERSION"
dpkg --unpack ipp-dsp-azkaban*.deb
dpkg --configure ipp-dsp-azkaban-"$EXECUTION_ENVIRONMENT"-"$AZKABAN_PACKAGE_VERSION"

mkdir -p /etc/authn/regression
cp /etc/authn/$EXECUTION_ENVIRONMENT/* /etc/authn/regression

tail -f /dev/null
