#!/bin/bash

SERVER_BIN=../../../../../service/cascade_server
CLIENT_BIN=../../../../../service/cascade_client

if [ $# != 1 ]; then
    echo "USAGE <client/server>"
    exit
fi

if [ $1 == 'client' ]; then
    ${CLIENT_BIN}
elif [ $1 == 'server' ]; then
    ${SERVER_BIN}
else
    echo "Unknown argument:$1, please use 'client' or 'server'"
fi
