package org.apache.flink.runtime.taskexecutor.rescale;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.rescale.rescalePlan;
import org.apache.flink.runtime.state.replication.*;
import org.apache.flink.util.FlinkException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static java.net.InetAddress.getByName;
import static java.net.InetAddress.getLocalHost;
import static org.apache.flink.runtime.state.replication.ReplicationManager.parseWorkerConfig;
import static org.apache.flink.runtime.state.replication.NetworkUtil.readn;

// 负责task manager侧
public class rescaleManager {
    protected static final Logger log = LoggerFactory.getLogger(rescaleManager.class);
    private static final Properties confProperties = ProcessLevelConf.getPasaConf();
    public static final int group = 3;
    public static final String topic = confProperties.getProperty(ProcessLevelConf.RESCLAE_TOPIC);
    public static final int port = 24444;

    rescalePlan plan;
    //    记录哪些节点和localhost在一个chaingroup
    Set<InetAddress> groupMembers = new HashSet<>();

    kafkaWorker kafkaWorker;
    InetAddress localhost;
    AbstractInvokable task;

    //  告诉别人本机有哪些状态
    private String getPubMsg(StateDescriptor info) {
        int pt = port + info.subtaskIndex;
        return "pub:" + info.toString() + ":" + localhost.getHostAddress() + ":" + pt;
    }

    //    告诉别人本机要获取哪些状态
    private String subMsg(JobVertexID jvid, int index, boolean isFull) {
        String tp = isFull ? "full" : "notfull";
        return "sub:" + jvid + ":" + index + ":" + tp + ":" + localhost.getHostAddress();
    }

    public rescaleManager(AbstractInvokable task) {
        try {
            List<String> workers = parseWorkerConfig();
            this.groupMembers = getGroupMembers(workers);
            for (InetAddress groupMember : groupMembers) {
                log.info("member:{}", groupMember.getHostAddress());
            }
            localhost = getLocalHost();
            kafkaWorker = new kafkaWorker(localhost.getHostName() + "rescale", topic);
            this.task = task;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<CompletableFuture<Void>> pubAndPost(StateDescriptor owned) throws InterruptedException {
        CompletableFuture<Void> future1 = new CompletableFuture<>();
        CompletableFuture<Void> future2 = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(future1);
        futures.add(future2);
        CompletableFuture.runAsync(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(port + owned.subtaskIndex);
                Socket conn = serverSocket.accept();
                InetAddress peer = conn.getInetAddress();
                boolean full = shouldSendFully(peer);
                byte[] data = task.getSnapshot(full).toBytes();
                future1.complete(null);
                byte[] len = int2byte(data.length);
                conn.getOutputStream().write(len);
                conn.getOutputStream().write(data);
            } catch (IOException | FlinkException e) {
                e.printStackTrace();
            } finally {
                future2.complete(null);
            }
        });
        String pubMsg = getPubMsg(owned);
        kafkaWorker.send(pubMsg);
        log.info("rescale pub and post:{}", port + owned.subtaskIndex);
        return futures;
    }

    public stateLocation sub(StateDescriptor wanted) throws IOException {
        log.info("sub state:{}", wanted.toString());
        while (true) {
            ConsumerRecords<String, String> records = kafkaWorker.get();
            for (ConsumerRecord<String, String> record : records) {
                log.info("get msg:{}", record.value());
                String[] msgs = record.value().split(":");
                assert msgs.length == 6;
                if (msgs[0].equals("pub")) {
                    stateLocation stateLocation = parsePubMsg(msgs);
                    if (wanted.equals(stateLocation.stateDescriptor)) {
                        log.info("should get from {}", stateLocation.host.getHostName());
                        return stateLocation;
                    }
                }
            }
        }
    }

    public byte[] fetch(StateDescriptor want, InetAddress where, int port) throws IOException {
        log.info("try fetch from:{}", port);
        int reties = 100;
        while (true) {
            Socket socket = null;
            try {
                socket = new Socket(where, port);
            } catch (IOException e) {
                reties--;
                if (reties == 0)
                    throw new IOException("retries too many");
                continue;
            }
            byte[] lenBuffer = new byte[4];
            InputStream inputStream = socket.getInputStream();
            readn(inputStream, lenBuffer, 4);
            int len = byte2int(lenBuffer);
            byte[] dataBuffer = new byte[len];
            readn(inputStream, dataBuffer, len);
            socket.close();
            return dataBuffer;
        }
    }

    private stateLocation parsePubMsg(String[] msgs) throws UnknownHostException {
        JobID jobID = JobID.fromHexString(msgs[1]);
        JobVertexID jvid = JobVertexID.fromHexString(msgs[2]);
        int index = Integer.parseInt(msgs[3]);
        InetAddress from = InetAddress.getByName(msgs[4]);
        return new stateLocation(jobID, jvid, index, from, port + index);
    }

    private Set<InetAddress> getGroupMembers(List<String> workers) throws UnknownHostException {
        InetAddress localhost = getLocalHost();
        Set<InetAddress> members = new HashSet<>();
        boolean found = false;
        for (int i = 0; i < workers.size(); i++) {
            if (i % group == 0) {
                if (found)
                    return members;
                members = new HashSet<>();
            }
            InetAddress addr = getByName(workers.get(i));
            members.add(addr);
            if (addr.equals(localhost))
                found = true;
        }
        return members;
    }

    public static byte[] int2byte(int number) {
        return ByteBuffer.allocate(4).putInt(number).array();
    }

    public static int byte2int(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    private boolean shouldSendFully(InetAddress peer) {
        return false;
    }

    public boolean shouldFull(InetAddress peer) {
        if (!groupMembers.contains(peer))
            return true;
        return peer.equals(localhost);
    }

    public void init() {
        kafkaWorker.init();
    }
}
