# flink-streaming-example
A complete apache flink streaming application that demonstrates computation of multiple failed login &amp; brute force attack alerts

JDK 1.8.0_144 or above
Flink 1.4.2
Kafka 0.10 or above
Scala 2.11

#To import into IDE such as eclipse
Just do maven project import

#To build
mvn clean package

Fat jar debflinkexample-0.0.1-SNAPSHOT.jar will be created. Use data file eventsForCreInitial_WithTenantID.txt plus this fat jar to run on cluster.
Once the job is started with the parameters given below, data from the above .txt file need to be pushed into the input Kafka topic 
[Prerequisite - need to have a Kafka cluster with the input & output topics created]

#To run on cluster
./bin/flink run -d -c com.deb.example.flinkstreaming.DebAppCEPKafka /scratch/dspathak/flinkcode/debsample-0.0.1-SNAPSHOT.jar --tenantIds tenant01 --consumerGroup deb_tenant01 --brokerList <host>:9092 --inputTopic flink2PartitionsCEPNew --outputTopic flink2PartitionsCEPOutput --envParallelism 2 --taskParallelism 4

./bin/flink run -d -c com.deb.example.flinkstreaming.DebAppCEPKafka /scratch/dspathak/flinkcode/debsample-0.0.1-SNAPSHOT.jar --tenantIds tenant02,tenant03 --consumerGroup deb_tenant0203 --brokerList <host>:9092 --inputTopic flink2PartitionsCEPNew --outputTopic flink2PartitionsCEPOutput --envParallelism 2 --taskParallelism 4

./bin/flink run -d -c com.deb.example.flinkstreaming.DebAppCEPKafka_Alternate /scratch/dspathak/flinkcode/debsample-0.0.1-SNAPSHOT.jar --tenantIds tenant01 --consumerGroup deb_alt_tenant01 --brokerList <host>:9092 --inputTopic flink2PartitionsCEPNew --outputTopic flink2PartitionsCEPOutputAlternative --envParallelism 2 --taskParallelism 4

#To run within eclipse IDE
Create a run configuration with Main class & Program arguments as given above. Make sure classpath in run configuration is set to pick up the jar from target folder.