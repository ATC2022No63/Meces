package org.apache.flink.runtime.taskexecutor.rescale;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.replication.StateDescriptor;

import java.net.InetAddress;

// 这就代表了state存放的位置
public class stateLocation {
    public StateDescriptor stateDescriptor;
    public InetAddress host;
    public int port;
    public boolean isFull;

    public stateLocation(JobID jid, JobVertexID jvid, int subtaskIndex, InetAddress host, int port) {
        this.stateDescriptor = new StateDescriptor(jid, jvid, subtaskIndex, null);
        this.host = host;
        this.port = port;
    }

    public void setHost(InetAddress host) {
        this.host = host;
    }
}
