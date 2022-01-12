package org.apache.flink.runtime.state.mirgration;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.microbatch.kafka.Subber;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.FlinkException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.flink.runtime.microbatch.ProcessConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class FetchWatcher {
    Subber subber;
    AbstractInvokable task;
    protected static final Logger log = LoggerFactory.getLogger(org.apache.flink.runtime.state.mirgration.FetchWatcher.class);

    public FetchWatcher(int subindex, AbstractInvokable invokable) {
        String groupid = "fetch_sub" + subindex + System.nanoTime();
        String topic = ProcessConf.getProperty(ProcessConf.FETCH_TOPIC);
        String host = ProcessConf.getProperty(ProcessConf.KAFKA_HOST);
        String timeout = ProcessConf.getProperty(ProcessConf.KAFKA_TIMEOUT);
        subber = new Subber(groupid, host, topic, timeout);
        task = invokable;
    }

    public void watch(String prefix) throws IOException, FlinkException {
        log.info("start fetch watch with pre:{}",prefix);
        CompletableFuture.runAsync(()->{
            while (true) {
                try {
                    ConsumerRecords<String, String> records = subber.get();
                    for (ConsumerRecord<String, String> record : records) {
                        String value = record.value();
                        log.info("get msg:{}",value);
                        String[] msg = value.split(":");
                        String _prefix = msg[0];
                        int kg = Integer.parseInt(msg[1]);
                        String address = msg[2];
                        int port = Integer.parseInt(msg[3]);
                        if (!prefix.equals(_prefix))
                            continue;
                        TaskInfo ti = task.getEnvironment().getTaskInfo();
                        int parallelism = ti.getNumberOfParallelSubtasks();
                        int myIndex = ti.getIndexOfThisSubtask();
                        int owner = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(128, parallelism, kg);
                        if (myIndex == owner)
                            task.sendState(kg, address, port);
                    }
                } catch (FlinkException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }


}
