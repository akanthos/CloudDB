#!/bin/bash    

kill -9 $(jps | grep jar | awk '$2 ~ /^jar$/ { print $1 }')
