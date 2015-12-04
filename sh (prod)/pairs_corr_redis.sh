export PYTHONPATH=/home/larry-13.04/workspace/finopt:$PYTHONPATH
KAFKA_ASSEMBLY_JAR=/home/larry-13.04/workspace/finopt/spark-streaming-kafka-assembly_2.10-1.4.1.jar
spark-submit  --jars  /home/larry-13.04/workspace/finopt/spark-streaming-kafka-assembly_2.10-1.4.1.jar /home/larry-13.04/workspace/finopt/cep/pairs_corr_redis.py vsu-01:2181 hkd-aud 

