#!/usr/bin/env bash

USER="clairvoyant"
HOST="10.10.4.76"
PORT="22"

scp -P ${PORT} target/spark-streaming-from-mqtt-jar-with-dependencies.jar ${USER}@${HOST}:/home/clairvoyant/spark_streaming_from_mqtt