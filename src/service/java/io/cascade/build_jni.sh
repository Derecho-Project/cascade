#!/usr/bin/bash
javac -h . Client.java Bundle.java QueryResults.java ServiceType.java ShardMemberSelectionPolicy.java CascadeObject.java ShardingPolicy.java

rm *.class
mv io_cascade_Client.h ../../jni/
rm *.h
