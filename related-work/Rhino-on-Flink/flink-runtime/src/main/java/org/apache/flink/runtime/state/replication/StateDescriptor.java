package org.apache.flink.runtime.state.replication;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;

import java.io.IOException;
import java.util.Arrays;

public class StateDescriptor {
    static final int IntSize = 4;
    private static final int MetaInfoSize = JobID.SIZE + JobVertexID.SIZE + 3 * IntSize;

    public final JobID jobID;
    public final JobVertexID jobVertexID;
    public final int subtaskIndex;
    public final KeyGroupRange keyGroupRange;
    public boolean isIncremental;

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

    public byte[] toBytes() throws IOException {
        DataOutputSerializer ds = new DataOutputSerializer(MetaInfoSize);
        ds.write(jobID.getBytes());
        ds.write(jobVertexID.getBytes());
        ds.writeInt(subtaskIndex);
        ds.writeInt(keyGroupRange.getStartKeyGroup());
        ds.writeInt(keyGroupRange.getEndKeyGroup());
        return ds.getCopyOfBuffer();
    }

    public StateDescriptor(byte[] bytes) throws IOException {
        DataInputDeserializer dd = new DataInputDeserializer(bytes);
        this.jobID = JobID.fromByteArray(Arrays.copyOfRange(bytes, 0, JobID.SIZE));
        this.jobVertexID = new JobVertexID(Arrays.copyOfRange(bytes, JobID.SIZE, JobID.SIZE * 2));
        dd.skipBytes(JobID.SIZE * 2);
        this.subtaskIndex = dd.readInt();
        int start = dd.readInt();
        int end = dd.readInt();
        this.keyGroupRange = new KeyGroupRange(start, end);
    }

    public static int getMetaInfoSize() {
        return MetaInfoSize;
    }

    public static int getIntSize() {
        return IntSize;
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

    public boolean isIncremental() {
        return isIncremental;
    }

    @Override
    public String toString() {
        return jobID + ":" + jobVertexID + ":" + subtaskIndex;
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
