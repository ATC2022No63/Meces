# Meces
Meces is a rescaling mechanism for stateful distributed stream processing systems.
This is the repository for Meces's implementation based on Apache Flink 1.12.0.

## Code Organization of This Project
```
- Meces-on-Flink: Meces implementation base on Flink.
- related-work
  - Megaphone-on-Flink: Megaphone implementations base on Flink.
  - Rhino-on-Flink: Rhino implementations base on Flink. 
```
## Meces on Flink
Meces code is in directory ```Meces-on-Flink```. Meces is compiled and deployed in the same way as Flink.

### Compile

To compile Meces:
```
mvn clean install -DskipTests -Dfast
```

To start a cluster:
```
cd Meces-on-Flink/build-target
./bin/start-cluster.sh
```

To stop the cluster:
```
cd Meces-on-Flink/build-target
./bin/stop-cluster.sh
```
### Preparation

Meces's rescaling stages workds with [Kafka](http://kafka.apache.org/) and [Redis](https://redis.io/).

For rescaling with Native Flink, a common practice calls for [HDFS](http://hadoop.apache.org/).

### Rescale Commnads

To rescaling operators of a Flink job, run the following command:
```
cd Meces-on-Flink/build-target
./bin/flink rescale -rmd 3 -rpl [<OPERATOR_NAME>:<PARALLELISM>]  <JOB_ID>
```
### Config and parameters

Flink configs can be set in file ```Meces-on-Flink/build-target/conf/flink-conf.yaml```.

Meces parameters are set in file ```Meces-on-Flink/build-target/conf/meces.conf.prop```. The contents are set to default values after completing the complation.

The main parameters of Meces are as follows:
``` shell
# Redis configs
redis.connection.pool.size=24
redis.db.index=2
redis.hosts.list=redis_host1,redis_host2,redis_host3
# Kafka configs
kafka.hosts.list=kafka_host:port
kafka.requestMessage.topic=postFetchRequestTopic
kafka.state.topic=postFetchStateTopic
kafka.stateInfo.topic=postFetchStateInfoTopic
# Test option, to switch between Partial-Pause and Meces
test.partialPause=false
# Meces settings for Sub-groups and Gradual Migration
state.numBins=128
state.batchSize=10
```

## Experiments
### benchmark jobs
The source of ```Nexmark``` and ```key-count``` jobs used in the paper can be found in ```Meces-on-Flink/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/rescale```.
The corresponding jars can be found in ```Meces-on-Flink/flink-examples/flink-examples-streaming/target``` after compiling the Meces project.

Typically, to run and rescale a ```key-count``` job, one can run the following commands:
``` shell
OPERATOR_NAME="FlatMap-Counter -> Appender -> Sink: Sink"

JAR_PATH=/ROOT_PATH/flink-examples/flink-examples-streaming/target/flink-examples-streaming_2.11-1.12.0-RescaleWordCount.jar

# parallelism
DEFAULT_PARA=25
COUNT_PARA=25

# params
INTERVAL=5
COUNTER_LOOPS=50
COUNTER_MAX_PAPA=128
SOURCE_RATE=40000
SINK_INTERVAL=1000
KAFKA_PORT=9092
KAFKA_SERVERS=host1:9092,host2:9092
TOPIC=test_par_25
EARLIEST=false
NATIVE_FLINK=false

# submit job
JOB_PARAMS="
-d \
-p ${DEFAULT_PARA} \
${JAR_PATH} \
-interval ${INTERVAL} \
-counterCostlyOperationLoops ${COUNTER_LOOPS} \
-KafkaPort ${KAFKA_PORT} \
-kafkaServers ${KAFKA_SERVERS} \
-topic ${TOPIC} \
-startFromEarliest ${EARLIEST} \
-counterPar ${COUNT_PARA} \
-counterMaxPara ${COUNTER_MAX_PAPA} \
-sinkInterval ${SINK_INTERVAL} \
-nativeFlink ${NATIVE_FLINK} \
-sourceRate ${SOURCE_RATE} \
-defaultPar ${DEFAULT_PARA}"


SUBMIT_OUTPUT=`/ROOT_PATH/build-target/bin/flink run ${JOB_PARAMS}`
JOB_ID=${SUBMIT_OUTPUT##*submitted with JobID }
echo ${JOB_ID}

sleep 500

# rescale
/ROOT_PATH/build-target/bin/flink rescale -rmd 3 -rpl ["${OPERATOR_NAME}":30]  ${JOB_ID}

```
### Megaphone on Flink
Code and usage of ```Megaphone on Flink``` in Section Evaluation is located at [Megaphone](https://github.com/ATC2022No63/Meces/tree/main/related-work/Megaphone-on-Flink).
### Rhino on Flink
Code and usage of ```Rhino on Flink``` in Section Evaluation is located at [Rhino](https://github.com/ATC2022No63/Meces/tree/main/related-work/Rhino-on-Flink).
