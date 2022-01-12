package org.apache.flink.runtime.state.replication;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ReplicationCoordinator {
    protected static final Logger log = LoggerFactory.getLogger(ReplicationCoordinator.class);
    private static final Properties confProperties = ProcessLevelConf.getPasaConf();
    private static final int interval = Integer.parseInt(confProperties.getProperty(ProcessLevelConf.REPLICATION_INTERVAL));
    private static final String topic = confProperties.getProperty(ProcessLevelConf.REPLICATION_TOPIC);
    private static final String enable = confProperties.getProperty(ProcessLevelConf.ENABLE_REPLICATION);

    private final kafkaWorker kafkaWorker;
    Set<TaskManagerGateway> groupLeaders;
    int epoch = 1;
    boolean running = true;

    public ReplicationCoordinator(Set<ExecutionVertex> executionVertices) {
        groupLeaders = new HashSet<>();
        this.kafkaWorker = new kafkaWorker("replication-coordinator", topic);
        try {
            this.groupLeaders = getGroupLeaders(executionVertices);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (enable.equals("off"))
            return;
        loop();
    }


    private static Set<TaskManagerGateway> getGroupLeaders(Set<ExecutionVertex> vertices) throws IOException {
        List<String> hostAddr = new ArrayList<>();
        for (String worker : ReplicationManager.parseWorkerConfig()) {
            hostAddr.add(InetAddress.getByName(worker).getHostAddress());
        }

        Set<TaskManagerGateway> leaders = new HashSet<>();
        String pattern = "\\d+\\.\\d+\\.\\d+\\.\\d+";

        for (ExecutionVertex executionVertex : vertices) {
            TaskManagerGateway manager = executionVertex.getCurrentExecutionAttempt().getTaskmanager();
            String managerAddr = manager.getAddress();
            Matcher matcher = Pattern.compile(pattern).matcher(managerAddr);
            if (matcher.find()) {
                String addr = matcher.group(0);
                int index = hostAddr.indexOf(addr);
                if (index == 0)
                    leaders.add(manager);
            } else {
                System.out.printf("illegal addr:%s\n", managerAddr);
            }
        }
        return leaders;
    }

    public void doReplication() throws ExecutionException, InterruptedException {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (TaskManagerGateway groupleader : groupLeaders) {
            CompletableFuture<Void> future = groupleader.startReplication(epoch);
            futures.add(future);
        }
        for (CompletableFuture<Void> future : futures) {
            future.get();
        }
    }

    public void loop() {
        CompletableFuture.runAsync(() -> {
            try {
                boolean first = true;

                while (running) {
                    if (first) {
                        first = false;
                        Thread.sleep(10000);
                    } else {
                        Thread.sleep(interval);
                    }
                    if (!running)
                        return;
                    log.info("replication epoch {} begin", epoch);
                    doReplication();
                    ack(epoch);
                    log.info("replication epoch {} over", epoch);
                    epoch++;
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    private void ack(int epoch) {
        String ackMsg = "ack:" + epoch;
        kafkaWorker.send(ackMsg);
    }

    public void disable() {
        running = false;
    }
}
