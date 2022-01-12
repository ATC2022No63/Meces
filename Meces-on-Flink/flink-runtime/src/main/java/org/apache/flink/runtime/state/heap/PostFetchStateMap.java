package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.pooled.PooledHashMap;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.redis.ProcessLevelConf;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * This implementation of {@link StateMap} uses {@link HashMap} objects.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of value.
 */
public class PostFetchStateMap<K, N, S> extends StateMap<K, N, S> {

	private static final boolean USE_POOLED_MAP = Boolean.parseBoolean(ProcessLevelConf.getProperty(ProcessLevelConf.COMMON_USE_POOLED_MAP));

	/**
	 * Map for holding the actual state objects. The nested map provide
	 * an outer scope by namespace and an inner scope by key.
	 */
	private final Map<N, Map<K, S>> namespaceMap;
	private TypeSerializer<N> namespaceSerializer;
	private TypeSerializer<K> keySerializer;
	private TypeSerializer<S> stateSerializer;

	/**
	 * Constructs a new {@code PostFetchStateMap}.
	 */
//	PostFetchStateMap(
//		TypeSerializer<N> namespaceSerializer,
//		TypeSerializer<K> keySerializer,
//		TypeSerializer<S> stateSerializer
//	) {
//		this.namespaceMap = new HashMap<>();
//	}
	PostFetchStateMap(
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<K> keySerializer,
		TypeSerializer<S> stateSerializer) {
		this.namespaceSerializer = namespaceSerializer;
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
		this.namespaceMap = new HashMap<>();
	}

	private Map<K, S> createStateMap() {
		if (USE_POOLED_MAP) {
			return new PooledHashMap<>(
				Integer.parseInt(ProcessLevelConf.getMecesConf().getProperty(ProcessLevelConf.COMMON_POOLED_MAP_POOL_SIZE_PER_THREAD)),
				namespaceSerializer.getClass(),
				keySerializer.getClass(),
				stateSerializer.getClass());
		} else {
			return new HashMap<>();
		}
//		return new HashMap<>();
	}

	// serialize API ------------------------------------------------------------------------------
	public byte[] toBytes(
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<K> keySerializer,
			TypeSerializer<S> stateSerializer) throws IOException {
		long now = System.currentTimeMillis();
		DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(calculateInitSize());
		int numberOfEntries = 0;
		for (Map<K, S> namespaceMap : namespaceMap.values()) {
			numberOfEntries += namespaceMap.size();
		}
		dataOutputSerializer.writeInt(numberOfEntries);
		long usedSum = 0;
		long usedMax = 0;
		for (Map.Entry<N, Map<K, S>> namespaceEntry : namespaceMap.entrySet()) {
			N namespace = namespaceEntry.getKey();
			for (Map.Entry<K, S> entry : namespaceEntry.getValue().entrySet()) {
				long here1 = System.currentTimeMillis();
				namespaceSerializer.serialize(namespace, dataOutputSerializer);
				keySerializer.serialize(entry.getKey(), dataOutputSerializer);
				stateSerializer.serialize(entry.getValue(), dataOutputSerializer);
				long here2 = System.currentTimeMillis();
				usedSum += here2 - here1;
				usedMax = Math.max(usedMax, here2 - here1);
			}
		}
		long now1 = System.currentTimeMillis();
		if (now1 - now > 1000) {
			// TODO: RESIZE?
			StringBuilder sb = new StringBuilder();
			sb
				.append("toBytes took too long: ")
				.append(now1 - now)
				.append("\ncalculateInitSize(): ")
				.append(calculateInitSize())
				.append("\nbuffer length: ")
				.append(dataOutputSerializer.getSharedBuffer().length)
				.append("\nnumberOfEntries")
				.append(numberOfEntries)
				.append("\nusedAve")
				.append(usedSum / numberOfEntries)
				.append("\nuseMax")
				.append(usedMax);
			System.out.println(sb.toString());
		}
		return dataOutputSerializer.getSharedBuffer();
//		ByteArrayOutputStream bis = new ByteArrayOutputStream();
//		ObjectOutputStream ois = new ObjectOutputStream(bis);
//		ois.writeObject(this);
//
//		ois.close();
//		bis.close();
//		return bis.toByteArray();
	}

	private int calculateInitSize() {
		// very roughly calculate the init size for Serialization buffer
		int size = 0;
		for (Map<K, S> kvMap : namespaceMap.values()) {
			for (Map.Entry<K, S> ignored : kvMap.entrySet()) {
				size += 1; // assume namespace takes 1 byte;
				size += 8; // assume keys and states are int, each takes 4 byte;
			}
		}
		return Math.max(size, 1); // at least 1 to make DataInputDeserializer happy
	}

	public void fromBytes(
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<K> keySerializer,
			TypeSerializer<S> stateSerializer,
			byte[] data) throws IOException {
		long now = System.currentTimeMillis();
		DataInputDeserializer dataInputDeserializer = new DataInputDeserializer(data);
		int numberOfEntries = dataInputDeserializer.readInt();
		for (int i = 0; i < numberOfEntries; i++) {
			try {
				N namespace = namespaceSerializer.deserialize(dataInputDeserializer);
				K key = keySerializer.deserialize(dataInputDeserializer);
				S state = stateSerializer.deserialize(dataInputDeserializer);
				// TODO: use pool map
				this.put(key, namespace, state);
			} catch (Exception e) {
				// TODO: window operator may fail here, fix it
			}
		}
		System.out.println("fromBytes took: " + (System.currentTimeMillis() - now) + " data length: " + data.length);
//		ByteArrayInputStream bis = new ByteArrayInputStream(data);
//		ObjectInputStream ois = new ObjectInputStream (bis);
//		Object obj = ois.readObject();
//		ois.close();
//		bis.close();
//		return (PostFetchStateMap) obj;
	}

	// Public API from StateMap ------------------------------------------------------------------------------

	@Override
	public int size() {
		int count = 0;
		for (Map<K, S> keyMap : namespaceMap.values()) {
			if (null != keyMap) {
				count += keyMap.size();
			}
		}

		return count;
	}

	@Override
	public S get(K key, N namespace) {
		Map<K, S> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		return keyedMap.get(key);
	}

	@Override
	public boolean containsKey(K key, N namespace) {
		Map<K, S> keyedMap = namespaceMap.get(namespace);

		return keyedMap != null && keyedMap.containsKey(key);
	}

	@Override
	public void put(K key, N namespace, S state) {
		putAndGetOld(key, namespace, state);
	}

	@Override
	public S putAndGetOld(K key, N namespace, S state) {
		Map<K, S> keyedMap = namespaceMap.computeIfAbsent(namespace, k -> createStateMap());

		return keyedMap.put(key, state);
	}

	@Override
	public void remove(K key, N namespace) {
		removeAndGetOld(key, namespace);
	}

	@Override
	public S removeAndGetOld(K key, N namespace) {
		Map<K, S> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		S removed = keyedMap.remove(key);

		if (keyedMap.isEmpty()) {
			namespaceMap.remove(namespace);
		}

		return removed;
	}

	@Override
	public <T> void transform(
		K key, N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {
		Map<K, S> keyedMap = namespaceMap.computeIfAbsent(namespace, k -> createStateMap());
		keyedMap.put(key, transformation.apply(keyedMap.get(key), value));
	}

	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		return new StateEntryIterator();
	}

	@Override
	public Stream<K> getKeys(N namespace) {
		return namespaceMap.getOrDefault(namespace, Collections.emptyMap()).keySet().stream();

	}

	@Override
	public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
		int recommendedMaxNumberOfReturnedRecords) {
		return new StateEntryVisitor();
	}

	@Override
	public int sizeOfNamespace(Object namespace) {
		Map<K, S> keyMap = namespaceMap.get(namespace);
		return keyMap != null ? keyMap.size() : 0;
	}

	@Nonnull
	@Override
	public StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> stateSnapshot() {
		return new PostFetchStateMapSnapshot<>(this);
	}

	public Map<N, Map<K, S>> getNamespaceMap() {
		return namespaceMap;
	}

	/**
	 * Iterator over state entries in a {@link PostFetchStateMap}.
	 */
	class StateEntryIterator implements Iterator<StateEntry<K, N, S>> {
		private Iterator<Map.Entry<N, Map<K, S>>> namespaceIterator;
		private Map.Entry<N, Map<K, S>> namespace;
		private Iterator<Map.Entry<K, S>> keyValueIterator;

		StateEntryIterator() {
			namespaceIterator = namespaceMap.entrySet().iterator();
			namespace = null;
			keyValueIterator = Collections.emptyIterator();
		}

		@Override
		public boolean hasNext() {
			return keyValueIterator.hasNext() || namespaceIterator.hasNext();
		}

		@Override
		public StateEntry<K, N, S> next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			if (!keyValueIterator.hasNext()) {
				namespace = namespaceIterator.next();
				keyValueIterator = namespace.getValue().entrySet().iterator();
			}

			Map.Entry<K, S> entry = keyValueIterator.next();

			return new StateEntry.SimpleStateEntry<>(
				entry.getKey(), namespace.getKey(), entry.getValue());
		}
	}


	/**
	 * Incremental visitor over state entries in a {@link PostFetchStateMap}.
	 *
	 * <p>The iterator keeps a snapshotted copy of key/namespace sets, available at the beginning of iteration.
	 * While further iterating the copy, the iterator returns the actual state value from primary maps
	 * if exists at that moment.
	 *
	 * <p>Note: Usage of this iterator can have a heap memory consumption impact.
	 */
	class StateEntryVisitor implements InternalKvState.StateIncrementalVisitor<K, N, S>, Iterator<StateEntry<K, N, S>> {
		private Iterator<Map.Entry<N, Map<K, S>>> namespaceIterator;
		private Map.Entry<N, Map<K, S>> namespace;
		private Iterator<Map.Entry<K, S>> keyValueIterator;
		private StateEntry<K, N, S> nextEntry;
		private StateEntry<K, N, S> lastReturnedEntry;

		StateEntryVisitor() {
			namespaceIterator = new HashSet<>(namespaceMap.entrySet()).iterator();
			namespace = null;
			keyValueIterator = null;
			nextKeyIterator();
		}

		@Override
		public boolean hasNext() {
			nextKeyIterator();
			return keyIteratorHasNext();
		}

		@Override
		public Collection<StateEntry<K, N, S>> nextEntries() {
			StateEntry<K, N, S> nextEntry = next();
			return nextEntry == null ? Collections.emptyList() : Collections.singletonList(nextEntry);
		}

		@Override
		public StateEntry<K, N, S> next() {
			StateEntry<K, N, S> next = null;
			if (hasNext()) {
				next = nextEntry;
			}
			nextEntry = null;
			lastReturnedEntry = next;
			return next;
		}

		private void nextKeyIterator() {
			while (!keyIteratorHasNext()) {
				if (namespaceIteratorHasNext()) {
					namespace = namespaceIterator.next();
					keyValueIterator = new HashSet<>(namespace.getValue().entrySet()).iterator();
				} else {
					break;
				}
			}
		}

		private boolean keyIteratorHasNext() {
			while (nextEntry == null && keyValueIterator != null && keyValueIterator.hasNext()) {
				Map.Entry<K, S> next = keyValueIterator.next();
				Map<K, S> ns = namespaceMap.getOrDefault(namespace.getKey(), null);
				S upToDateValue = ns == null ? null : ns.getOrDefault(next.getKey(), null);
				if (upToDateValue != null) {
					nextEntry = new StateEntry.SimpleStateEntry<>(next.getKey(), namespace.getKey(), upToDateValue);
				}
			}
			return nextEntry != null;
		}

		private boolean namespaceIteratorHasNext() {
			return namespaceIterator.hasNext();
		}

		@Override
		public void remove() {
			remove(lastReturnedEntry);
		}

		@Override
		public void remove(StateEntry<K, N, S> stateEntry) {
			namespaceMap.get(stateEntry.getNamespace()).remove(stateEntry.getKey());
		}

		@Override
		public void update(StateEntry<K, N, S> stateEntry, S newValue) {
			namespaceMap.get(stateEntry.getNamespace()).put(stateEntry.getKey(), newValue);
		}
	}
}
