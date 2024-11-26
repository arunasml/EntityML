#!/usr/bin/zsh

source ./env.sh

sudo rm -rf ${COMPOSE_LOG_PATH}/broker
sudo rm -rf ${COMPOSE_LOG_PATH}/zookeeper

./prepare.sh