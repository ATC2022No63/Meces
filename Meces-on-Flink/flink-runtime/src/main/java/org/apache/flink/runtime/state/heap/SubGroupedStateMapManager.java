package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.pooled.PooledHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubGroupedStateMapManager<K, N, S> {
	private final PostFetchStateMap<K, N, S> borrowedKeyBinState;

	public enum KeyGroupStatus {
		EMPTY, PARTIAL_FETCHED, FETCHED
	}

	private static class KeyGroupStateMap<K, N, S> {
		Map<Integer, StateMap<K, N, S>> data;
		KeyGroupStatus keyGroupStatus = KeyGroupStatus.FETCHED;

		KeyGroupStateMap(Map<Integer, StateMap<K, N, S>> data) {
			this.data = data;
		}

		KeyGroupStateMap() {
			this(new HashMap<>());
		}
	}

	private static class KeyBinStruct<K, N, S> {
		K key;
		int keyBin;
		int keyGroup;
		N namespace;

		KeyBinStruct(K key, int keyBin, int keyGroup, N namespace) {
			this.key = key;
			this.keyBin = keyBin;
			this.keyGroup = keyGroup;
			this.namespace = namespace;
		}
	}

	private final KeyBinStruct<K, N, S> keyBinStructInUse = new KeyBinStruct<>(null, -1, -1, null);
	private final KeyBinStruct<K, N, S> keyBinStructPosting = new KeyBinStruct<>(null, -1, -1, null);

	private final int numBins;
	// stateMaps: Maps keyGroupIndex to KeyGroupStateMaps
	// KeyGroupStateMaps: one KeyGroupStateMap maps subKeyRange to StateMaps
	// StateMaps: one StateMap maps keys to states
	private Map<Integer, KeyGroupStateMap<K, N, S>> stateMaps;
	private final PostFetchStateTable<K, N, S> table;

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<N> namespaceSerializer;
	private final TypeSerializer<S> stateSerializer;

	@SuppressWarnings("unchecked")
	SubGroupedStateMapManager(
		int numBins,
		PostFetchStateTable<K, N, S> table,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer,
		List<KeyGroupRange> keyGroupRanges) {
		stateMaps = new HashMap<>();
		for (KeyGroupRange keyGroupRange : keyGroupRanges) {
			for (int keyGroup : keyGroupRange) {
				stateMaps.put(keyGroup, new KeyGroupStateMap<>());
			}
		}
		this.numBins = numBins;
		this.table = table;

		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.stateSerializer = stateSerializer;
		borrowedKeyBinState = new PostFetchStateMap<>(namespaceSerializer, keySerializer, stateSerializer);
	}

	void markFetchingKeyGroupsAsFetching(List<KeyGroupRange> keyGroupRanges) {
		for (KeyGroupRange keyGroupRange : keyGroupRanges) {
			for (int keyGroup : keyGroupRange) {
				if (!stateMaps.containsKey(keyGroup)) {
					KeyGroupStateMap<K, N, S> keyGroupStateMap = new KeyGroupStateMap<>();
					keyGroupStateMap.keyGroupStatus = KeyGroupStatus.EMPTY;
					stateMaps.put(keyGroup, keyGroupStateMap);
				}
			}
		}
	}

	boolean isKeyBinEmpty(int keyGroupIndex, K key) {
		KeyGroupStateMap keyGroupStateMap = getStateMap(keyGroupIndex);
		if (keyGroupStateMap != null) {
			return !keyGroupStateMap.data.containsKey(keyToBinHash(key));
		} else {
			return true;
		}
	}

	boolean isKeyBinHashEmpty(int keyGroupIndex, int keyBinHash) {
		KeyGroupStateMap keyGroupStateMap = getStateMap(keyGroupIndex);
		if (keyGroupStateMap != null) {
			return !keyGroupStateMap.data.containsKey(keyBinHash);
		} else {
			return true;
		}
	}

	int getNumBins() {
		return numBins;
	}

	KeyGroupStatus getKeyGroupStatus(int keyGroupIndex) {
		KeyGroupStateMap<K, N, S> keyGroupStateMap = getStateMap(keyGroupIndex);
		if (keyGroupStateMap == null) {
			return KeyGroupStatus.EMPTY;
		} else {
			return keyGroupStateMap.keyGroupStatus;
		}
	}

	boolean isKeyGroupEmpty(int keyGroupIndex) {
		return getKeyGroupStatus(keyGroupIndex) != KeyGroupStatus.EMPTY;
	}

	StateMap<K, N, S> getMap(K key, int keyGroupIndex) {
		return getMapFromBin(keyToBinHash(key), keyGroupIndex);
	}

	private StateMap<K, N, S> getMapFromBin(int binIndex, int keyGroupIndex) {
		KeyGroupStateMap<K, N, S> keyGroupStateMap = getStateMap(keyGroupIndex);
		// always check isKeyGroupEmpty before calling this
		if (keyGroupStateMap == null) {
			throw new UnknownError("this whole keyGroup is not fetched");
		}

		StateMap<K, N, S> keyBinStateMap = keyGroupStateMap.data.get(binIndex);
		if (keyBinStateMap == null) {
			if (keyGroupStateMap.keyGroupStatus == KeyGroupStatus.FETCHED) {
				keyBinStateMap = table.createStateMap();
				keyGroupStateMap.data.put(binIndex, keyBinStateMap);
			}
		}
		return keyBinStateMap;
	}

	void putStateMap(StateMap<K, N, S> stateMap, int keyGroupIndex, K key) {
		int keyBinIndex = keyToBinHash(key);
		getStateMap(keyGroupIndex).data.put(keyBinIndex, stateMap);
	}

	int keyToBinHash(K key) {
		// TODO: the key can be null when restore from savepoint, handle this
		// it could be caused by empty windows
		// reproduced by nexmark q3
		if (key == null) {
			return 0;
		}
		return key.hashCode() % numBins;
	}

	void clean(List<KeyGroupRange> keyGroupRanges) {
		for (KeyGroupRange keyGroupRange : keyGroupRanges) {
			for (int keyGroup : keyGroupRange) {
				stateMaps.remove(keyGroup);
			}
		}
	}

	byte[] keyGroupToBytes(int keyGroupIndex) throws IOException {
		Map<Integer, StateMap<K, N, S>> subStateMap = getStateMap(keyGroupIndex).data;

		// TODO: pre-calculate the size
		DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(12);
		int numberOfEntries = subStateMap.size();
		dataOutputSerializer.writeInt(numberOfEntries);
		for (Map.Entry<Integer, StateMap<K, N, S>> entry : subStateMap.entrySet()) {
			int subKeyGroup = entry.getKey();
			dataOutputSerializer.writeInt(subKeyGroup);
			byte[] stateMapData = ((PostFetchStateMap<K, N, S>) entry.getValue()).toBytes(namespaceSerializer, keySerializer, stateSerializer);
			dataOutputSerializer.writeInt(stateMapData.length);
			dataOutputSerializer.write(stateMapData);
		}
		return dataOutputSerializer.getSharedBuffer();
	}

	byte[][] keyGroupToBytesInKeyBins(int keyGroupIndex) throws IOException {
		List<byte[]> result = new ArrayList<>(this.numBins);
		for (int i = 0; i < numBins; i++) {
			result.add(i, new byte[0]);
		}
		try {
			Map<Integer, StateMap<K, N, S>> subStateMap = getStateMap(keyGroupIndex).data;
			for (Map.Entry<Integer, StateMap<K, N, S>> entry : subStateMap.entrySet()) {
				Integer binHash = entry.getKey();
				byte[] data;
				PostFetchStateMap<K, N, S> stateMap = (PostFetchStateMap<K, N, S>) entry.getValue();
				if (stateMap != null) {
					data = stateMap.toBytes(namespaceSerializer, keySerializer, stateSerializer);
				} else {
					data = new byte[0];
				}
				result.add(binHash, data);
			}
		} catch (Exception e) {
			// TODO: ConcurrentModificationException should not occur, maybe window issue
		}

		return result.toArray(new byte[0][0]);
	}

	byte[] keyBinToBytes(int keyGroupIndex, K key) throws IOException {
		return keyBinHashToBytes(keyGroupIndex, keyToBinHash(key));
	}

	byte[] keyBinHashToBytes(int keyGroupIndex, int keyBinIndex) {
		try {
			long now = System.currentTimeMillis();
			StateMap<K, N, S> subStateMap = getStateMap(keyGroupIndex).data.get(keyBinIndex);
			long now1 = System.currentTimeMillis() - now;
			if (now1 - now > 1000) {
				System.out.println("keyBinHashToBytes took too long: " + (now1 - now));
			}
			if (subStateMap != null) {
				return ((PostFetchStateMap<K, N, S>) subStateMap).toBytes(
					namespaceSerializer,
					keySerializer,
					stateSerializer);
			} else {
				return new byte[0];
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new byte[0];
		}
	}

	private KeyGroupStateMap<K, N, S> getStateMap(int keyGroupIndex) {
		KeyGroupStateMap<K, N, S> map = stateMaps.get(keyGroupIndex);
		if (map == null) {
			// it should not reach here,
			// because it should have been initiated in the constructor function
			// bug reproduced in nexmark7
			// TODO: find why
			map = new KeyGroupStateMap<>();
			stateMaps.put(keyGroupIndex, map);
		}
		return map;
	}
	void storeFetchedKeyBinState(int keyGroupIndex, byte[] data, K key) throws IOException {
		storeFetchedKeyBinHashState(keyGroupIndex, data, keyToBinHash(key));
	}

	void storeFetchedKeyBinHashState(int keyGroupIndex, byte[] data, int keyBinIndex) throws IOException {
		long now = System.currentTimeMillis();
		PostFetchStateMap<K, N, S> stateMap = getDeserializedStateMap(data);
		long now1 = System.currentTimeMillis() - now;
		if (getKeyGroupStatus(keyGroupIndex) == KeyGroupStatus.EMPTY) {
			stateMaps.put(keyGroupIndex, new KeyGroupStateMap<>());
			getStateMap(keyGroupIndex).keyGroupStatus = KeyGroupStatus.PARTIAL_FETCHED;
		}
		long now2 = System.currentTimeMillis() - now;
		getStateMap(keyGroupIndex).data.put(keyBinIndex, stateMap);
		long now3 = System.currentTimeMillis() - now;
//		if (now3 > 1000) {
//			long here1 = System.currentTimeMillis();
//			PostFetchStateMap<K, N, S> tmp = getDeserializedStateMap(data);
//			long here2 = System.currentTimeMillis();
//			System.out.println("now 1: " + now1 + "\tnow 2: " + now2 + "\tnow 3: " + now3 + "\t2nd try took: " + (here2 - here1) + "\t" + tmp);
//		}
	}

	private PostFetchStateMap<K, N, S> getDeserializedStateMap(byte[] data) throws IOException {
		PostFetchStateMap<K, N, S> stateMap = new PostFetchStateMap<>(namespaceSerializer, keySerializer, stateSerializer);
		if (data.length > 0) {
			stateMap.fromBytes(
				namespaceSerializer,
				keySerializer,
				stateSerializer,
				data);
		}
		return stateMap;
	}

	private KeyGroupStateMap<K, N, S> bytesToKeyGroup(byte[] data) throws IOException {
		DataInputDeserializer dataInputDeserializer = new DataInputDeserializer(data);

		Map<Integer, StateMap<K, N, S>> subStateMap = new HashMap<>();
		int numberOfEntries = dataInputDeserializer.readInt();
		for (int i = 0; i < numberOfEntries; i++) {
			int subKeyGroup = dataInputDeserializer.readInt();
			int stateMapDataLength = dataInputDeserializer.readInt();
			byte[] stateMapData = new byte[stateMapDataLength];
			dataInputDeserializer.read(stateMapData);
			PostFetchStateMap<K, N, S> stateMap = new PostFetchStateMap<>(namespaceSerializer, keySerializer, stateSerializer);
			stateMap.fromBytes(
				namespaceSerializer,
				keySerializer,
				stateSerializer,
				stateMapData);
			subStateMap.put(subKeyGroup, stateMap);
		}
		return new KeyGroupStateMap<>(subStateMap);
	}

	void storeFetchedKeyGroupState(int keyGroupIndex, byte[] data) throws IOException {
		if (data == null) {
			throw new UnknownError();
		}
		KeyGroupStateMap<K, N, S> keyGroupStateMap = getStateMap(keyGroupIndex);
		if (keyGroupStateMap == null) {
			synchronized (this) {
				stateMaps.put(keyGroupIndex, bytesToKeyGroup(data));
			}
		} else {
			synchronized (this) {
				KeyGroupStateMap<K, N, S> fetchedKeyGroupStateMap = bytesToKeyGroup(data);
				for (Map.Entry<Integer, StateMap<K, N, S>> entry : fetchedKeyGroupStateMap.data.entrySet()) {
					if (!keyGroupStateMap.data.containsKey(entry.getKey())) {
						keyGroupStateMap.data.put(entry.getKey(), entry.getValue());
					}
				}
				keyGroupStateMap.keyGroupStatus = KeyGroupStatus.FETCHED;
			}
		}
	}

	void markKeyBinAsBorrowed(int keyGroupIndex, K key) {
		getStateMap(keyGroupIndex).data.put(keyToBinHash(key), borrowedKeyBinState);
	}

	void markKeyBinHashAsBorrowed(int keyGroupIndex, int keyBinHash) {
		getStateMap(keyGroupIndex).data.put(keyBinHash, borrowedKeyBinState);
	}

	boolean isKeyBinBorrowed(StateMap<K, N, S> stateMap) {
		return stateMap == borrowedKeyBinState;
	}

	synchronized void setInUse(K key, int keyGroup, N namespace) {
		keyBinStructInUse.key = key;
		keyBinStructInUse.keyBin = keyToBinHash(key);
		keyBinStructInUse.keyGroup = keyGroup;
		keyBinStructInUse.namespace = namespace;
	}

	synchronized void setPosting(int keyBin, int keyGroup, N namespace) {
		keyBinStructPosting.keyBin = keyBin;
		keyBinStructPosting.keyGroup = keyGroup;
		keyBinStructPosting.namespace = namespace;
	}

	synchronized void releasePosting() {
		keyBinStructPosting.keyBin = -1;
		keyBinStructPosting.keyGroup = -1;
		keyBinStructPosting.namespace = null;
	}

	boolean isInUse(int keyGroup, int keyBin) {
		return keyBinStructInUse.keyGroup == keyGroup && keyBinStructInUse.keyBin == keyBin;
	}

	boolean isPosting(int keyGroup, int keyBin) {
		return keyBinStructPosting.keyGroup == keyGroup && keyBinStructPosting.keyBin == keyBin;
	}
}
