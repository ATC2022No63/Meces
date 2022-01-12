package org.apache.flink.runtime.state.mirgration;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;

import java.io.IOException;
import java.util.Arrays;

public class StateDescriptor {

    public final JobID jobID;
    public final JobVertexID jobVertexID;
    public final int subtaskIndex;
    public final KeyGroupRange keyGroupRange;
    public String name;

    public StateDescriptor(
            JobID jobID,
            JobVertexID jobVertexID,
            int subtask,
            KeyGroupRange keyGroupRange) {
        this.jobID = jobID;
        this.jobVertexID = jobVertexID;
        this.subtaskIndex = subtask;
        this.keyGroupRange = keyGroupRange;
    }

    public void setName(String name) {
        this.name = name;
    }

    public byte[] toBytes() throws IOException {
        System.out.printf("deserial descriptor %s\n",toString());
        DataOutputSerializer ds = new DataOutputSerializer(1024);
        ds.write(jobID.getBytes());
        ds.write(jobVertexID.getBytes());
        ds.writeInt(subtaskIndex);
        ds.writeInt(keyGroupRange.getStartKeyGroup());
        ds.writeInt(keyGroupRange.getEndKeyGroup());
        StringSerializer ss = new StringSerializer();
        ss.serialize(name, ds);
        return ds.getCopyOfBuffer();
    }

    public StateDescriptor(DataInputDeserializer dd) throws IOException {
        byte[] jidBuffer = new byte[JobID.SIZE];
        byte[] jvidBuffer = new byte[JobVertexID.SIZE];
        dd.readFully(jidBuffer);
        dd.readFully(jvidBuffer);
        this.jobID = new JobID(jidBuffer);
        this.jobVertexID = new JobVertexID(jvidBuffer);
        this.subtaskIndex = dd.readInt();
        int start = dd.readInt();
        int end = dd.readInt();
        this.keyGroupRange = new KeyGroupRange(start, end);
        StringSerializer ss = new StringSerializer();
        this.name = ss.deserialize(dd);
    }

    public JobID getJobID() {
        return jobID;
    }

    public JobVertexID getJobVertexID() {
        return jobVertexID;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }


    @Override
    public String toString() {
        return jobID + ":" + jobVertexID + ":" + subtaskIndex + ":" + name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StateDescriptor that = (StateDescriptor) o;
        return subtaskIndex == that.subtaskIndex && jobID.equals(that.jobID) && jobVertexID.equals(that.jobVertexID);
    }

    @Override
    public int hashCode() {
        return jobVertexID.hashCode() + subtaskIndex;
    }
}
