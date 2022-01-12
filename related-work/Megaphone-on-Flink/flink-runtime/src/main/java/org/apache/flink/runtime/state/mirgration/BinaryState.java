package org.apache.flink.runtime.state.mirgration;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class BinaryState {
    OperatorStatePartition[] states;

    public BinaryState(OperatorStatePartition[] s) {
        this.states = s;
    }

    public byte[] toBytes() throws IOException {
        DataOutputSerializer ds = new DataOutputSerializer(1024);
        ds.writeInt(states.length);
        for (OperatorStatePartition state : states) {
            byte[] s = state.toBytes();
            ds.writeInt(s.length);
            ds.write(s);
        }
        return ds.getCopyOfBuffer();
    }
}
