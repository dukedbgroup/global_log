#!/bin/bash
echo "Testing "

mvn exec:java -e -Dexec.mainClass="edu.duke.globallog.sparklogprocessor.TestRelM" -Dexec.cleanupDaemonThreads=false
