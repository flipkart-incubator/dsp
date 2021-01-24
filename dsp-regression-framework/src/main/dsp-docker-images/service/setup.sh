#!/bin/bash


rm /etc/apt/sources.list
echo "deb http://0.0.0.0/debian jessie main" > /etc/apt/sources.list
echo "deb http://0.0.0.0/debian-security/ jessie/updates main" >> /etc/apt/sources.list

#Installing mail server
sudo debconf-set-selections <<< "postfix postfix/mailname string localhost"
sudo debconf-set-selections <<< "postfix postfix/main_mailer_type string 'Internet Site'"
sudo apt-get -y --force-yes install fk-3p-mail
sudo /usr/bin/add_sasl.sh dfp-notifications ba9c6cbd

/etc/init.d/postfix reload
echo "0.0.0.0 github.fkinternal.com" |  tee -a /etc/hosts

#Changing debian version
ls -lh  /etc/apt/sources.list.d/ipp-dsp-service.list
REPO_NAME="${EXECUTION_ENVIRONMENT}-ipp-dsp-service-repo"
if grep -q $REPO_NAME "/etc/apt/sources.list.d/ipp-dsp-service.list"; then
  sed -i '/'"$REPO_NAME"'/c\deb [trusted=yes] http://0.0.0.0/repos/'"$REPO_NAME"'/'"$SERIVCE_DEBIAN_VERSION"' /' /etc/apt/sources.list.d/ipp-dsp-service.list
else
  echo "deb [trusted=yes] http://0.0.0.0/repos/"$REPO_NAME"/"$SERIVCE_DEBIAN_VERSION" /" > /etc/apt/sources.list.d/ipp-dsp-service.list
fi

apt-get update

cat  /etc/temp_t >> /etc/hosts

apt-get purge ipp-dsp-service

apt-get install --yes --allow-unauthenticated stream-relay

apt-get update
apt-get download ipp-dsp-service
sudo dpkg --unpack ipp-dsp-service*.deb
sudo dpkg --configure  ipp-dsp-service

apt-get install --yes --allow-unauthenticated ipp-dsp-service

sed -i 's/'"$EXECUTION_ENVIRONMENT"'/'"$SERVICE_BUCKET_POSTFIX"'/' /etc/default/ipp-dsp-service.env

chmod 777 /etc/init.d/dsp-service.sh
/etc/init.d/dsp-service.sh start
tail -f /dev/null