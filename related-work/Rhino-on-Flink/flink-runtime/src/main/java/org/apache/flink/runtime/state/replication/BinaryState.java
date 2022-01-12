package org.apache.flink.runtime.state.replication;

import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.Map;

// ------------------------------------------------------------------------
// @date: 2021/6/29
// 对应task executor上所有算子状态序列化（因为增量状态以te为粒度传输）
// ------------------------------------------------------------------------
public class BinaryState {
    Map<StateDescriptor, OperatorStatePartition> states;


    public BinaryState(Map<StateDescriptor, OperatorStatePartition> s) {
        this.states = s;
    }

    public byte[] toBytes() throws IOException {
        if (states.size() == 0)
            return new byte[0];
        DataOutputSerializer ds = new DataOutputSerializer(1024);
        ds.writeInt(states.size());
        for (Map.Entry<StateDescriptor, OperatorStatePartition> entry : states.entrySet()) {
            ds.write(entry.getKey().toBytes());
            byte[] s = entry.getValue().toBytes();
            ds.writeInt(s.length);
            ds.write(s);
        }
        return ds.getCopyOfBuffer();
    }

    public Map<StateDescriptor, OperatorStatePartition> getStates() {
        return states;
    }
}

