package org.apache.flink.runtime.state.replication;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

// ------------------------------------------------------------------------
// @date: 2021/6/29
// 对应算子状态的一部分（通常是某个subtask的状态）！
// ------------------------------------------------------------------------
public class OperatorStatePartition<K, N, V> {
    Map<String, StatePartition> states;

    public OperatorStatePartition(Map<String, StatePartition> states) {
        this.states = states;
    }

    public byte[] toBytes() throws IOException {
        DataOutputSerializer ds = new DataOutputSerializer(1024);
        ds.writeInt(states.size());
        StringSerializer ss = new StringSerializer();
        for (Map.Entry<String, StatePartition> e : states.entrySet()) {
            ss.serialize(e.getKey(), ds);
            byte[] s = e.getValue().toBytes();
            ds.writeInt(s.length);
            ds.write(s);
        }
        return ds.getCopyOfBuffer();
    }
}
