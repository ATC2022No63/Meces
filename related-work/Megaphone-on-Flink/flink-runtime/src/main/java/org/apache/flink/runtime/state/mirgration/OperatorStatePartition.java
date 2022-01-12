package org.apache.flink.runtime.state.mirgration;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OperatorStatePartition<N, K, V> {
    final Map<N, Map<K, V>> state;

    TypeSerializer<N> namespaceSerializer;
    TypeSerializer<K> keySerializer;
    TypeSerializer<V> stateSerializer;
    int subindex;
    String name;

    public OperatorStatePartition(
            String n,
            int subindex,
            Map<N, Map<K, V>> state,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> stateSerializer
    ) {
        this.name = n;
        this.subindex = subindex;
        this.state = state;
        this.namespaceSerializer = namespaceSerializer;
        this.keySerializer = keySerializer;
        this.stateSerializer = stateSerializer;
    }


    public byte[] toBytes() throws IOException {
        DataOutputSerializer ds = new DataOutputSerializer(1024);
        int entries = 0;
        for (Map<K, V> value : state.values()) {
            entries += value.size();
        }
        ds.writeUTF(name);
        ds.writeInt(subindex);
        ds.writeInt(entries);
        for (Map.Entry<N, Map<K, V>> nMapEntry : state.entrySet()) {
            Map<K, V> kvs = nMapEntry.getValue();
            N ns = nMapEntry.getKey();
            for (Map.Entry<K, V> entry : kvs.entrySet()) {
                namespaceSerializer.serialize(ns, ds);
                keySerializer.serialize(entry.getKey(), ds);
                stateSerializer.serialize(entry.getValue(), ds);
            }
        }
        return ds.getCopyOfBuffer();
    }

    public static <N, K, V> Map<N, Map<K, V>> fromBytes(TypeSerializer<N> namespaceSerializer, TypeSerializer<K> keySerializer,
                                                        TypeSerializer<V> stateSerializer, byte[] bytes) throws IOException {
        Map<N, Map<K, V>> state = new HashMap<>();
        DataInputDeserializer dd = new DataInputDeserializer(bytes);
        int nentry = dd.readInt();
        for (int j = 0; j < nentry; j++) {
            N ns = namespaceSerializer.deserialize(dd);
            K key = keySerializer.deserialize(dd);
            V value = stateSerializer.deserialize(dd);
            state.computeIfAbsent(ns, n -> new HashMap<K, V>()).put(key, value);
        }
        return state;
    }
}
