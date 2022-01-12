package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the snapshot of a {@link PostFetchStateMap}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class PostFetchStateMapSnapshot<K, N, S>
	extends StateMapSnapshot<K, N, S, PostFetchStateMap<K, N, S>> {

	/**
	 * Creates a new {@link PostFetchStateMapSnapshot}.
	 *
	 * @param owningStateMap the {@link PostFetchStateMap} for which this object represents a snapshot.
	 */
	public PostFetchStateMapSnapshot(PostFetchStateMap<K, N, S> owningStateMap) {
		super(owningStateMap);
	}

	@Override
	public void writeState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer,
		@Nonnull DataOutputView dov,
		@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) throws IOException {
		Map<N, Map<K, S>> mappings = filterMappingsIfNeeded(owningStateMap.getNamespaceMap(), stateSnapshotTransformer);
		int numberOfEntries = countMappingsInKeyGroup(mappings);

		dov.writeInt(numberOfEntries);
		for (Map.Entry<N, Map<K, S>> namespaceEntry : mappings.entrySet()) {
			N namespace = namespaceEntry.getKey();
			for (Map.Entry<K, S> entry : namespaceEntry.getValue().entrySet()) {
				namespaceSerializer.serialize(namespace, dov);
				keySerializer.serialize(entry.getKey(), dov);
				stateSerializer.serialize(entry.getValue(), dov);
			}
		}
	}

	private Map<N, Map<K, S>> filterMappingsIfNeeded(
		final Map<N, Map<K, S>> keyGroupMap,
		StateSnapshotTransformer<S> stateSnapshotTransformer) {
		if (stateSnapshotTransformer == null) {
			return keyGroupMap;
		}

		Map<N, Map<K, S>> filtered = new HashMap<>();
		for (Map.Entry<N, Map<K, S>> namespaceEntry : keyGroupMap.entrySet()) {
			N namespace = namespaceEntry.getKey();
			Map<K, S> filteredNamespaceMap = filtered.computeIfAbsent(namespace, n -> new HashMap<>());
			for (Map.Entry<K, S> keyEntry : namespaceEntry.getValue().entrySet()) {
				K key = keyEntry.getKey();
				S transformedvalue = stateSnapshotTransformer.filterOrTransform(keyEntry.getValue());
				if (transformedvalue != null) {
					filteredNamespaceMap.put(key, transformedvalue);
				}
			}
			if (filteredNamespaceMap.isEmpty()) {
				filtered.remove(namespace);
			}
		}

		return filtered;
	}

	private int countMappingsInKeyGroup(final Map<N, Map<K, S>> keyGroupMap) {
		int count = 0;
		for (Map<K, S> namespaceMap : keyGroupMap.values()) {
			count += namespaceMap.size();
		}

		return count;
	}
}

