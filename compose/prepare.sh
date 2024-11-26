#!/usr/bin/zsh

source ./env.sh
echo Logging set to ${COMPOSE_LOG_PATH}

# Prevent default user creating the log folders, since when different from uid within the containers they won't have write permissions.
mkdir -p ${COMPOSE_LOG_PATH}/broker
mkdir -p ${COMPOSE_LOG_PATH}/zookeeper/data
mkdir -p ${COMPOSE_LOG_PATH}/zookeeper/log

# nuclear escape
sudo chmod 777 -R ${COMPOSE_LOG_PATH}

