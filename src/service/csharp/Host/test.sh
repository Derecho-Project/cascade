#!/bin/bash

mcs -t:library -out:TestUDL.dll *.cs
g++ -o Host.out -D LINUX jsmn.c GatewayToManaged.cpp SampleHost.cpp -ldl
./Host.out
rm TestUDL.dll