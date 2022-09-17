#!/bin/sh

mcs -t:library HelloWorldUDL.cs
g++ -o Host.out -D LINUX jsmn.c GatewayToManaged.cpp SampleHost.cpp -ldl
./Host.out
