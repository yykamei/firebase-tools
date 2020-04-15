#!/bin/bash
while :
do
  /usr/bin/time --format="$(date)\t%E real\t%U user\t%S sys" curl https://fredzqm-staging.firebaseio-staging.com/xxx.json -s > /dev/null
	sleep 1
done
