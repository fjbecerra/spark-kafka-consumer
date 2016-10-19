# spark-kafka-consumer

$ export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

$ spark-submit --class com.pakius.DirectKafkaConsumer --master local[*] --driver-memory 1G --executor-memory 1G target/scala-2.11/simple-consumer-spark_2.11-1.0.jar