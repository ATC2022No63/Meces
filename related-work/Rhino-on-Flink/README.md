## Rhino on Flink

This project is implemented based on the incremental state transfer mechanism proposed by [Rhino](https://dl.acm.org/doi/10.1145/3318464.3389723) . As the authors of Rhino have not open-sourced it by the time we submit this paper, we choose to implement it based on its description by ourselves.

Our version of Rhino relies on `Kafka` to synchronize replication/rescaling procedure and you should deploy Kafka on your cluster before you running it.

This implementation consists of two parts, namely incremental replication of state and state migration. We put the configuration file in `{{project_path}}/build-target/conf/rhino-conf` and you should configure  it according to your own environment.

### Compilation

To compile Rhino-on-Flink:
```
mvn clean install -DskipTests -Dfast
```

To start a cluster:
```
cd {{Megaphone-on-Flink target directory}}
./bin/start-cluster.sh
```

To stop the cluster:
```
cd {{Megaphone-on-Flink target directory}}
./bin/stop-cluster.sh
```

### Experiment
- what to run

  We use the representative key-count job for evaluation. The source code of the job used can be found in Rhino-on-Flink/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/rescale. The corresponding jars can be found in Megaphone-on-flink/flink-examples/flink-examples-streaming/target after compiling project.

- how to run

  ```python
  # python3
  jvm_opt = {"-C": {{kafka_connector_jar_path}}, "-d": ""}
  
  job_opt = {
      # kafka source data topic
      "topic": "wordcount2",
      # kafka server hosts
      "kafkaServers": "slave205:9092,slave206:9092",
      # interval for printing results (in ms)
      "sinkInterval": 1000,
      # upstream operator of counter parallelism     
      "defaultPar": 10,
      # counter parallelism
      "counterPar": 20,
      "counterCostlyOperationLoops": 1000,
  }
  
  jvm_comfig = " ".join([f"{k} {v}" for k, v in jvm_opt.items()])
  job_config = " ".join([f"--{k} {v}" for k, v in job_opt.items()])
  submit_cmd = f"./flink run {jvm_comfig} {rescale_wordcount_jar} {job_config}"
  os.chdir({{flink_bin_path}})
  output = subprocess.check_output(submit_cmd, shell=True).decode('utf-8')
  pattern = re.compile("Job has been submitted with JobID (.{32})")
  jobid = pattern.search(output)[1]
  ```

Trigger state migration:

```
cd {{Megaphone-on-Flink target directory}}
./bin flink rescale {{job-id}}
```
### Requirements

Our version of implementation relies on `Kafka` to synchronize replication/rescaling procedure, and you should deploy Kafka on your cluster before running it.

We put the configuration file in `{{project_path}}/build-target/conf/rhino-conf` and you should configure it according to your own environment.