### Megaphone on Flink

Megaphone is published in [Megaphone: latency-conscious state migration for distributed streaming dataflows](https://dl.acm.org/doi/10.14778/3329772.3329777)
and is a state migration approach that splits the state load into batches and embed the migration flows into data flows for lower latency. It relies on two
specific SPE features:

	(1) state extraction from upstream operator 
	
	(2) dataflow frontiers. 

However, both are still not natively supported in many widely-used SPEs, including Flink. Therefore, to run Megaphone in a widelyused SPE, we meet the above
requirements of Megaphone in Flink by adding RPC calls for state extraction and splitting the data stream into micro batches underhook. Based on that, we
implement Megaphone’s state migration mechanism on Flink.

#### Installation

Megaphone on Flink is implemented on Flink so the compilation and deployment is the same as original Flink.

Compilation:

```
cd {{Megaphone-on-Flink target directory}}
mvn clean install -DskipTests -Dfast
```

Deployment:

```
cd {{Megaphone-on-Flink target directory}}/build-target
./bin/start-cluster.sh
```

Run experiment:

- what to run

  We use the representative key-count job for evaluation. The source code of the job used can be found in Megaphone-on-Flink/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/rescale. The corresponding jars can be found in Megaphone-on-flink/flink-examples/flink-examples-streaming/target after compiling project.

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

#### Requirements

Our version of implementation relies on `Kafka` to synchronize replication/rescaling procedure and you should deploy Kafka on your cluster before running it.

We put the configuration file in `{{project_path}}/build-target/conf/megaphone-conf` and you should configure it according to your own environment.