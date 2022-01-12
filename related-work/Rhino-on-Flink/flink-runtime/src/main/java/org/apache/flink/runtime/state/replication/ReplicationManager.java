package org.apache.flink.runtime.state.replication;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static java.net.InetAddress.getByName;
import static java.net.InetAddress.getLocalHost;

public class ReplicationManager {
    protected static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private static Properties confProperties = ProcessLevelConf.getPasaConf();

    private static final String ENV_CONF = "CONF_DIR";
    private static final String ENV_STATE = "STATE_DIR";
    public static final String topic = confProperties.getProperty(ProcessLevelConf.REPLICATION_TOPIC);

    private static final int ReplicationPort = Integer.parseInt(confProperties.getProperty(ProcessLevelConf.REPLICATION_PORT));

    kafkaWorker kafkaWorker;
    private final InetAddress mine;
    private final InetAddress peer;
    private final TaskExecutor executor;
    public String prefix;
    int epoch = 0;
    List<File> files = new ArrayList<>();

    public void watchRequest() {
        log.info("start watching request");
        try {
            while (true) {
                ConsumerRecords<String, String> records = this.kafkaWorker.get();
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    log.info("get msg:{}", value);
                    String[] msg = value.split(":");
                    String tp = msg[0];
                    if (tp.equals("ack")) {
                        int newEpoch = Integer.parseInt(msg[1]);
                    } else if (tp.equals("pub")) {
                        InetAddress from = getByName(msg[1]);
                        InetAddress to = getByName(msg[2]);
                        int newEpoch = Integer.parseInt(msg[3]);
                        if (!to.equals(mine))
                            continue;
                        doReplication(from, newEpoch);
                    } else
                        log.error("unknown msg type:{}", tp);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.warn("should not go here");
    }

    public ReplicationManager(TaskExecutor taskExecutor) throws IOException {
        this.executor = taskExecutor;
        List<String> workers = parseWorkerConfig();

        mine = getLocalHost();
        peer = getPeer(workers);
        this.kafkaWorker = new kafkaWorker(topic + mine.getHostName(), topic);
        log.info("init ReplicationWorker ip {} peer {},topic:{} ", mine, peer, topic);
        CompletableFuture.runAsync(this::watchRequest);
    }

    public void startReplication(int epoch) throws IOException, InterruptedException {
        if (this.epoch >= epoch) {
            log.warn("cur epoch:{} supposed:{}", this.epoch, epoch - 1);
            throw new IOException("illegal epoch received");
        }
        this.epoch = epoch;
        byte[] localState = getLocalState(epoch);
        log.info("local state size:{}", localState.length);
        pubAndPost(localState);
    }


    private void pubAndPost(byte[] tosend) {
        String msg = getPubMsg();
        CompletableFuture.runAsync(() -> {
            log.info("pub and post-port:{}", ReplicationPort);
            NetworkUtil.post(ReplicationPort, tosend);
        });
        this.kafkaWorker.send(msg);
    }

    public void doReplication(InetAddress from, int newEpoch) throws IOException, InterruptedException {
        log.info("request from {},epoch {}", from, newEpoch);
        final byte[] received = NetworkUtil.fetch(ReplicationPort, from);
        CompletableFuture.runAsync(() -> {
            parseAndSave(received);
        });
        if (newEpoch == epoch) {
            ack();
            return;
        }
        assert epoch == newEpoch - 1;
        epoch++;
        byte[] local = getLocalState(epoch);
        log.info("received {},local state size:{}", received.length, local.length);

        byte[] merged = mergeBytes(received, local);
        pubAndPost(merged);
    }

    @Override
    public String toString() {
        return "ReplicationWorker{" +
                "  mine=" + mine +
                ", peer=" + peer +
                ", port=" + ReplicationPort +
                '}';
    }

    public static List<String> parseWorkerConfig() throws IOException {
        List<String> workers = new ArrayList<>();
        final File confFile = new File(System.getenv(ENV_CONF), "workers");
        FileInputStream inputStream = new FileInputStream(confFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        while (reader.ready()) {
            workers.add(reader.readLine());
        }

        return workers;
    }

    private String getPubMsg() {
        return "pub:" + mine.getHostAddress() + ":" + peer.getHostAddress() + ":" + epoch;
    }


    private static InetAddress getPeer(List<String> workers) throws UnknownHostException {
        List<InetAddress> machines = new ArrayList<>();
        for (String worker : workers) {
            InetAddress byName = getByName(worker);
            machines.add(byName);
            log.info("groups:{}",byName.getHostName());
        }

        InetAddress me = getLocalHost();
        int where = machines.indexOf(me);
        assert where != -1 : "localhost not find?";

        int peerIndex = (where + 1) % workers.size();
        assert peerIndex >= 0 && peerIndex < machines.size() : "illegal peer index";
        return machines.get(peerIndex);
    }

    private static byte[] mergeBytes(byte[] received, byte[] local) {
        byte[] buffer = new byte[received.length + local.length];
        System.arraycopy(received, 0, buffer, 0, received.length);
        System.arraycopy(local, 0, buffer, received.length, local.length);
        return buffer;
    }


    private void ack() {
        log.info("epoch:{} completed", epoch);
    }

    private void parseAndSave(byte[] bytes) {
        if (prefix == null)
            prefix = "test-" + System.nanoTime();
        String filename = prefix + "-" + epoch;
        saveSafely(filename, bytes);
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

    private static String getStateName(StateDescriptor descriptor) {
        return String.valueOf(descriptor.jobID) + '-' + descriptor.jobVertexID + '-' + descriptor.subtaskIndex;
    }

    private void saveSafely(String fileName, byte[] payload) {
        try {
            parseBinaryState(payload);
            final File dir = new File(System.getenv(ENV_STATE));
            final File file = File.createTempFile(fileName, "", dir);
            assert !file.exists();
            file.deleteOnExit();
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            fileOutputStream.write(payload);
            fileOutputStream.flush();
            files.add(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] getLocalState(int epoch) throws IOException {
        try {
            return executor.getSnapshot(false).toBytes();
        } catch (IOException e) {
            return new byte[0];
        }
    }

    public int getEpoch() {
        return epoch;
    }

    private void parseBinaryState(byte[] bytes) throws IOException {
        DataInputDeserializer dd = new DataInputDeserializer(bytes);
        while (dd.available() > 0) {
            int nstate = dd.readInt();
            for (int i = 0; i < nstate; i++) {
                byte[] metaHeader = new byte[StateDescriptor.getMetaInfoSize()];
                dd.read(metaHeader);
                StateDescriptor descriptor = new StateDescriptor(metaHeader);
                int len = dd.readInt();
                dd.skipBytes(len);
                log.info("get incremental\njid:{},jvid:{},subindex:{}", descriptor.jobID, descriptor.jobVertexID, descriptor.subtaskIndex);
            }
        }
        assert dd.available() == 0;
    }

    public List<byte[]> getIncrementalSnapshot(StateDescriptor descriptor) throws IOException {
        log.info("get incremental until epoch:{}", epoch);
        List<byte[]> buffer = new ArrayList<>();
        for (File file : files) {
            FileInputStream inputStream = new FileInputStream(file);
            Long len = file.length();
            byte[] dataBuffer = new byte[len.intValue()];
            inputStream.read(dataBuffer);
            buffer.add(dataBuffer);
        }
        return buffer;
    }


}
