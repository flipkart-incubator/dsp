#!/bin/bash

mysqld &

mysql_ready() {
        mysqladmin ping --user=root > /dev/null 2>&1
    }
while !(mysql_ready)
do
       sleep 3
       echo "waiting for mysql ..."
done
mysqladmin -u root password password
mysql -uroot -ppassword -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'password' ; FLUSH PRIVILEGES;"

mysql -uroot -ppassword -e "create database dsp_regression_azkaban;"

mysql -uroot -ppassword  dsp_regression_azkaban < ipp_azkaban_2019-12-06.sql

tail -f /dev/null