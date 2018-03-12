#!/bin/bash
#build/mvn -T 16 -DskipTests=true -pl common/network-common,core,assembly package
build/mvn -T 16 -DskipTests=true -pl core,streaming,assembly package -e
#build/mvn -T 16 -DskipTests=true -pl core,assembly package -e
#build/mvn -T 16 -DskipTests=true -pl examples,assembly package
