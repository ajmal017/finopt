#!/bin/bash
ROOT=$FINOPT_HOME
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
KAFKA_ASSEMBLY_JAR=$FINOPT_HOME/jar/spark-streaming-kafka-assembly_2.10-1.4.1.jar
spark-submit  --jars  $KAFKA_ASSEMBLY_JAR /home/larry-13.04/workspace/finopt/src/alerts/nb_test.py

