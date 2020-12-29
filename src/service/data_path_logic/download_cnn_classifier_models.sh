#!/bin/bash
for name in flower pet
do
    wget -c https://derecho.cs.cornell.edu/files/${name}-model.tar.bz2 --no-check-certificate
    tar -jxf ${name}-model.tar.bz2
    rm -f ${name}-model.tar.bz2
done
