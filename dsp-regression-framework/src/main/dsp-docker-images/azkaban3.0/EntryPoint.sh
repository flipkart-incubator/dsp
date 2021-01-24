#!/bin/bash

sed -i '/mysql.database/c\mysql.database=dsp_azkaban' /usr/share/azkaban-exec-server-3.57.0/conf/azkaban.properties
sed -i '/mysql.host/c\mysql.host=mysql_compose' /usr/share/azkaban-exec-server-3.57.0/conf/azkaban.properties
sed -i '/mysql.user/c\mysql.user=root' /usr/share/azkaban-exec-server-3.57.0/conf/azkaban.properties
sed -i '/mysql.password/c\mysql.password=password' /usr/share/azkaban-exec-server-3.57.0/conf/azkaban.properties
sed -i '/azkaban.webserver.url/c\azkaban.webserver.url=https://'"$HOST_MACHINE_IP"':8443' /usr/share/azkaban-exec-server-3.57.0/conf/azkaban.properties

// Webserver configuration
sed -i '/mysql.database/c\mysql.database=dsp_azkaban' /usr/share/azkaban-web-server-3.57.0/conf/azkaban.properties

//notice mysql.host is set with the docker name of mysql which is defined in compose file
sed -i '/mysql.host/c\mysql.host=mysql_compose' /usr/share/azkaban-web-server-3.57.0/conf/azkaban.properties
sed -i '/mysql.user/c\mysql.user=root' /usr/share/azkaban-web-server-3.57.0/conf/azkaban.properties
sed -i '/mysql.password/c\mysql.password=password' /usr/share/azkaban-web-server-3.57.0/conf/azkaban.properties
sed -i '/jetty.port/c\jetty.port=8443' /usr/share/azkaban-web-server-3.57.0/conf/azkaban.properties

/etc/init.d/azkaban-exec-server start
/etc/init.d/azkaban-web-server start

echo 'deb [trusted=yes] http://0.0.0.0/repos/regression-ipp-dsp-azkaban-repo/'"$REGRESSION_AZKABAN_DEBIAN_VERSION"' /' > /etc/apt/sources.list.d/fk-azkaban.list

rm /etc/apt/sources.list
echo "deb http://0.0.0.0/debian jessie main" > /etc/apt/sources.list
echo "deb http://0.0.0.0/debian-security/ jessie/updates main" >> /etc/apt/sources.list
apt-get update
apt-get install --yes --allow-unauthenticated ipp-dsp-azkaban

tail -f /dev/null

