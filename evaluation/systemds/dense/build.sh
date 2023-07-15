#!/bin/bash

rm -r project/project; rm -r project/target; rm -r target
cp $SYSTEMDS_ROOT/target/SystemDS.jar lib/
sbt assembly