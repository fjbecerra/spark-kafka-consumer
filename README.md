# spark-kafka-consumer

####Docker:

 Kafka, zookeeper, schema-registry.

`>docker run --name kafka -it -p 2181:2181 -p 3030:3030 -p 8081:8081 -p 8082:8082 -p 8083:8083 -p 9092:9092 \
           -e ADV_HOST=<ip docker machine> \
           landoop/fast-data-dev`

####Mysql:
`>docker run --name my-container-name -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql/mysql-server:tag`


####Spark:
`>docker run --name spark -it -p 8088:8088 -p 8042:8042 -h --link mysql --link kafka sandbox -v $HOME/sparkApp:/app  sequenceiq/spark:1.6.0 bash`


 To Debug remotely.
`># export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005`

 Run Jobs.
`># spark-submit --class com.pakius.RDBInitializer --master local[*] --driver-memory 1G --executor-memory 1G /app/spark-kafka-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar /app/users.txt`
`># spark-submit --class com.pakius.EventPlayMusicPublisher --master local[*] --driver-memory 1G --executor-memory 1G /app/spark-kafka-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar /app/xaa.txt`
`># spark-submit --class com.pakius.DirectKafkaConsumer --master local[*] --driver-memory 1G --executor-memory 1G /app/spark-kafka-consumer-0.0.1-SNAPSHOT-jar-with-dependencies.jar`