#!/bin/sh

#git pull
rm -rf lib
mvn clean package -Dmaven.test.skip=true
cd bin
sh startup.sh -cf ../conf/config.properties
