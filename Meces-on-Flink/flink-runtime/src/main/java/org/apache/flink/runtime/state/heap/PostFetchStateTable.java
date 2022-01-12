package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.RescaleState;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.redis.BasicKVDatabaseClient;
import org.apache.flink.runtime.state.redis.ProcessLevelConf;
import org.apache.flink.runtime.state.redis.ShardedRedisClusterClient;
import org.apache.flink.runtime.state.rescale.SubTaskMigrationInstruction;
import org.apache.flink.runtime.taskexecutor.rpc.RpcRescalingResponder;

import redis.clients.jedis.Jedis;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.state.KeyGroupRangeDifferenceComputation.getDisposeAndFetching;
import static org.apache.flink.runtime.state.heap.PostFetchByteConverter.keyGroupAndBinHashToBytes;
import static org.apache.flink.runtime.state.heap.PostFetchByteConverter.keyGroupIndexToBytes;

/**
 * This implementation of {@link StateTable} uses {@link PostFetchStateMap}.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
public class PostFetchStateTable<K, N, S> extends StateTable<K, N, S> {

	private List<KeyGroupRange> sortedKeyGroupsInCharge;
	private List<KeyGroupRange> tmpKeyGroupsInCharge;
	private List<KeyGroupRange> sortedKeyGroupsToDispose = new ArrayList<>();
	private List<KeyGroupRange> sortedKeyGroupsFetching = new ArrayList<>();
	private List<KeyGroupRange> sortedKeyGroupsRemained = new ArrayList<>();
	private LinkedList<Integer> fetchingKeyGroups = new LinkedList<>();
	private boolean isFetching = false;
	private boolean isDisposing = false;
	private boolean toBeCleaned = false;
	private BasicKVDatabaseClient dbClient;
	private List<KeyGroupRange> previousKeyGroupsInCharge;
	private SubGroupedStateMapManager<K, N, S> subGroupedStateMapManager;

	private static Properties confProperties = ProcessLevelConf.getMecesConf();
	private static final int FETCH_INTERVAL_MILLI = Integer.parseInt(confProperties.getProperty(ProcessLevelConf.STATE_FETCH_INTERVAL_MILLI));
	private static final int TEST_REDIS_READING_COST_MILLI = Integer.parseInt(confProperties.getProperty(ProcessLevelConf.TEST_REDIS_READING_COST_MILLI_KEY));
	private static final int TEST_REDIS_WRITING_COST_MILLI = Integer.parseInt(confProperties.getProperty(ProcessLevelConf.TEST_REDIS_WRITING_COST_MILLI_KEY));
	private static final int MAX_FETCH_TIMES = Integer.parseInt(confProperties.getProperty(ProcessLevelConf.STATE_MAX_FETCH_TIMES_KEY));
	private static final int NUM_BINS = Integer.parseInt(confProperties.getProperty(ProcessLevelConf.STATE_NUM_BINS));
	private static final boolean CREATE_NEW_STATE_WHEN_FETCH_FAIL = false;
	private static final boolean TEST_PAUSE_WHEN_REDISTRIBUTING_STATE = Boolean.parseBoolean(confProperties.getProperty(ProcessLevelConf.TEST_PARTIAL_PAUSE));
	private static final List<KeyGroupRange> EMPTY_LIST = new ArrayList<>();
	private static final boolean OUTPUT_INFO = Boolean.parseBoolean(ProcessLevelConf.getProperty(ProcessLevelConf.OUTPUT_INFO));

	private final Object partialPauseLock = new Object();
	private final Object disposingLock = new Object();
	private final Object keyBinInUseLock = new Object();
	private final Object keyBinPostingLock = new Object();
	// TODO: rescalingResponder's taskName can be wrong, find out why
	private RpcRescalingResponder rescalingResponder;
	private String taskName;
	int subtaskIndex = -1;
	private N currentNamespace;
	private PostFetchRequestManager postFetchRequestManager;

	/**
	 * Creates a new {@link PostFetchStateTable} for the given key context and meta info.
	 * @param keyContext the key context.
	 * @param metaInfo the meta information for this state table.
	 * @param keySerializer the serializer of the key.
	 */

	@SuppressWarnings("unchecked")
	PostFetchStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer) {
		super(keyContext, metaInfo, keySerializer);
		try {
			dbClient = ShardedRedisClusterClient.getProcessLevelClient();
			postFetchRequestManager = PostFetchRequestManager.getInstance();
			subGroupedStateMapManager = new SubGroupedStateMapManager(
				NUM_BINS,
				this,
				keySerializer,
				getMetaInfo().getNamespaceSerializer(),
				getMetaInfo().getStateSerializer(),
				Collections.singletonList(keyContext.getKeyGroupRange()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

//	private void prepareJedis() {
//		Jedis jedis = new Jedis("slave203", 6379);
//		jedis.get
//	}

	@Override
	protected PostFetchStateMap<K, N, S> createStateMap() {
		return new PostFetchStateMap<>(getNamespaceSerializer(), keySerializer, getStateSerializer());
	}

	void markStateKeyGroups(List<KeyGroupRange> keyGroupsInCharge, RescaleState rescaleState, RpcRescalingResponder rescalingResponder, int subtaskIndex, String taskName) {
//		Preconditions.checkArgument(!isDisposing);
//		Preconditions.checkArgument(!isFetching);

		this.rescalingResponder = rescalingResponder;
		this.subtaskIndex = subtaskIndex;
		postFetchRequestManager.registerTableAndTryStartWatch(this);
		this.taskName = taskName;

		if (keyGroupsInCharge == null) {
			if (rescaleState == RescaleState.REMOVED) {
				this.sortedKeyGroupsToDispose = copyOf(this.sortedKeyGroupsInCharge, true);
			} else {
				if (this.sortedKeyGroupsInCharge == null) {
					// if keyGroupsInCharge is null and the operator is not to be removed, use current key groups as keyGroupsInCharge
					// this should happen only in the first prepare stage
					this.sortedKeyGroupsInCharge = new ArrayList<>();
					this.sortedKeyGroupsInCharge.add(keyContext.getKeyGroupRange());
				}
			}
		} else {
			if (this.sortedKeyGroupsInCharge != null && !this.sortedKeyGroupsInCharge.isEmpty()) {
				// we mark the new states held by this operator as to be fetched
				// we mark the states previously but no longer held by this operator as to be disposed
				sortedKeyGroupsFetching.clear();
				sortedKeyGroupsToDispose.clear();

				getDisposeAndFetching(sortedKeyGroupsInCharge, keyGroupsInCharge, sortedKeyGroupsToDispose, sortedKeyGroupsFetching, sortedKeyGroupsRemained);
			} else {
				// it has no state before, it must be a newly added instance
				sortedKeyGroupsFetching = copyOf(keyGroupsInCharge, true);
				sortedKeyGroupsToDispose.clear();
			}
			this.tmpKeyGroupsInCharge = copyOf(keyGroupsInCharge, true);
			this.subGroupedStateMapManager.markFetchingKeyGroupsAsFetching(sortedKeyGroupsFetching);
		}
		outputKeyGroups();
	}

	CompletableFuture enableMarkedStateKeyGroups(
		RescaleState rescaleState,
		SubTaskMigrationInstruction instruction) {
		if (rescaleState == RescaleState.ALIGNING) {
			if (TEST_PAUSE_WHEN_REDISTRIBUTING_STATE) {
				isFetching = !sortedKeyGroupsFetching.isEmpty();
				isDisposing = !sortedKeyGroupsToDispose.isEmpty();
				// TODO: do real aligning here
			} else {
				if (instruction != null) {
					postFetchRequestManager.tryUpdate(instruction, this.subtaskIndex);
				}
				return CompletableFuture.completedFuture(Acknowledge.get());
			}
		}

		synchronized (this) {
			if (rescaleState == RescaleState.REMOVED) {
				return handleRemovedSubtask();
			}

			try {
				updateStateMaps();
			} catch (Exception e) {
				e.printStackTrace();
				outputKeyGroups();
			}

			this.previousKeyGroupsInCharge = copyOf(sortedKeyGroupsInCharge, true);
//			this.sortedKeyGroupsInCharge = copyOf(tmpKeyGroupsInCharge, true);

			isFetching = !sortedKeyGroupsFetching.isEmpty();
			isDisposing = !sortedKeyGroupsToDispose.isEmpty();

			if (TEST_PAUSE_WHEN_REDISTRIBUTING_STATE) {
				postDisposedStateMapSync();
				fetchStateMapSync();
				sortedKeyGroupsInCharge = copyOf(tmpKeyGroupsInCharge, true);
				synchronized (partialPauseLock) {
					partialPauseLock.notifyAll();
				}
				sortedKeyGroupsRemained.clear();
				sortedKeyGroupsToDispose.clear();
				tmpKeyGroupsInCharge.clear();
				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				if (instruction == null) {
					sortedKeyGroupsRemained.clear();
					sortedKeyGroupsToDispose.clear();
					tmpKeyGroupsInCharge.clear();
				}
//				else {
//					postFetchRequestManager.update(instruction, this.subtaskIndex);
//				}

				return CompletableFuture
					.allOf(
						postDisposedStateMapAsync(instruction),
						fetchStateMapAsync(instruction))
					.whenCompleteAsync((a, t) -> {
						if (instruction == null) {
							sortedKeyGroupsRemained.clear();
							sortedKeyGroupsToDispose.clear();
							tmpKeyGroupsInCharge.clear();
						}
						postFetchRequestManager.finishCurrentFetchStage(this.subtaskIndex);
						log(this.subtaskIndex + "-finish enable.");
					});
			}
		}
	}

	private CompletableFuture sleep(long time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	private CompletableFuture handleRemovedSubtask() {
		this.sortedKeyGroupsToDispose = copyOf(sortedKeyGroupsInCharge, true);
		this.previousKeyGroupsInCharge = sortedKeyGroupsInCharge;
//		this.sortedKeyGroupsInCharge = EMPTY_LIST;

		if (TEST_PAUSE_WHEN_REDISTRIBUTING_STATE) {
			postDisposedStateMapSync();
			synchronized (partialPauseLock) {
				partialPauseLock.notifyAll();
			}

			sortedKeyGroupsRemained.clear();
			sortedKeyGroupsToDispose.clear();
			if (tmpKeyGroupsInCharge != null) {
				// TODO: use a clearKeyGroupList() function to the check-not-null and clear operation
				// for now, just keep it as it is, to indicate potential null-pointer bugs
				tmpKeyGroupsInCharge.clear();
			}
			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			return CompletableFuture
				.allOf(postDisposedStateMapAsync())
				.whenCompleteAsync((a, t) -> {
					sortedKeyGroupsRemained.clear();
					sortedKeyGroupsToDispose.clear();
					if (tmpKeyGroupsInCharge != null) {
						// TODO: use a clearKeyGroupList() function to the check-not-null and clear operation
						// for now, just keep it as it is, to indicate potential null-pointer bugs
						tmpKeyGroupsInCharge.clear();
					}
				});
		}
	}

	byte[] activelyPostKeyGroupState(int keyGroupIndex) {
		throw new UnsupportedOperationException();
	}

	void activelyPostKeyBinState(byte[] keyData) {
//		log("receive: " + System.currentTimeMillis());
		CompletableFuture.runAsync(() -> {
			try {
				postKeyBinState(keyData);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	private void updateStateMaps() {
		int newNumKeyGroups = 0;
		for (KeyGroupRange keyGroupRange : tmpKeyGroupsInCharge) {
			newNumKeyGroups += keyGroupRange.getNumberOfKeyGroups();
		}

		int oldPointer = 0, newPointer = 0, oldIndex = 0, newIndex = 0;
		if (sortedKeyGroupsRemained != null) {
//			for (KeyGroupRange keyGroupRange : sortedKeyGroupsRemained) {
//				while (!sortedKeyGroupsInCharge.get(oldPointer).contains(keyGroupRange.getStartKeyGroup())) {
//					oldPointer++;
//					oldIndex += sortedKeyGroupsInCharge.get(oldPointer).getNumberOfKeyGroups() + 1;
//				}
//				while (!tmpKeyGroupsInCharge.get(newPointer).contains(keyGroupRange.getStartKeyGroup())) {
//					newPointer++;
//					newIndex += tmpKeyGroupsInCharge.get(newPointer).getNumberOfKeyGroups() + 1;
//				}
//				int oldPosStart = keyGroupRange.getStartKeyGroup() - sortedKeyGroupsInCharge.get(oldPointer).getStartKeyGroup() + oldIndex;
//				int newPosStart = keyGroupRange.getStartKeyGroup() - tmpKeyGroupsInCharge.get(newPointer).getStartKeyGroup() + newIndex;
//				if (keyGroupRange.getNumberOfKeyGroups() + 1 >= 0) {
//					subGroupedStateMapManager.copyFromCurrentToTmpStateMaps(
//						oldPosStart,
//						newPosStart,
//						keyGroupRange.getNumberOfKeyGroups()
//					);
//					log("Copy " + keyGroupRange.getNumberOfKeyGroups() + " elements"
//						+ "from old stateMap starting at " + oldPosStart
//						+ " to new stateMap starting at " + newPosStart);
//				}
//			}
			sortedKeyGroupsRemained.clear();
		}
	}

	private boolean isKeyGroupInList(int keyGroupIndex, List<KeyGroupRange> keyGroups) {
		if (keyGroups == null || keyGroups.isEmpty()) {
			return false;
		}
		for (KeyGroupRange keyGroupRange : keyGroups) {
			if (keyGroupRange.contains(keyGroupIndex)) {
				return true;
			}
		}
		return false;
	}

	synchronized void outputKeyGroups() {
		log("\tsortedKeyGroupsInCharge:");
		outputKeyGroups(sortedKeyGroupsInCharge);
		log("\ttmpKeyGroupsInCharge:");
		outputKeyGroups(tmpKeyGroupsInCharge);
		log("\tsortedKeyGroupsFetching:");
		outputKeyGroups(sortedKeyGroupsFetching);
		log("\tsortedKeyGroupsToDispose:");
		outputKeyGroups(sortedKeyGroupsToDispose);
		log("\tsortedKeyGroupsRemained:");
		outputKeyGroups(sortedKeyGroupsRemained);
	}

	private void outputKeyGroups(List<KeyGroupRange> keyGroupRanges) {
		if (keyGroupRanges == null) {
			log("\t\tnull.");
		} else if (keyGroupRanges.isEmpty()) {
			log("\t\tempty.");
		} else {
			for (KeyGroupRange keyGroupRange : keyGroupRanges) {
				log("\t\t" + keyGroupRange.toString());
			}
		}
	}

	@Override
	public S get(N namespace) {
		return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	private S get(K key, int keyGroupIndex, N namespace) {
		checkKeyNamespacePreconditions(key, namespace);

		currentNamespace = namespace;

		try {
			while (subGroupedStateMapManager.isPosting(keyGroupIndex, subGroupedStateMapManager.keyToBinHash(key))) {
				synchronized (keyBinPostingLock) {
					keyBinPostingLock.wait();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		subGroupedStateMapManager.setInUse(keyContext.getCurrentKey(), keyGroupIndex, this.currentNamespace);
		synchronized (keyBinInUseLock) {
			keyBinInUseLock.notifyAll();
		}
		StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);

		if (stateMap == null) {
			return null;
		}

		return stateMap.get(key, namespace);
	}

	@Override
	public void put(K key, int keyGroup, N namespace, S state) {
		checkKeyNamespacePreconditions(key, namespace);

		synchronized (this) {
			StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);
			stateMap.put(key, namespace, state);
		}
	}

	@Override
	StateMap<K, N, S> getMapForKeyGroup(int keyGroupIndex) {
		if (TEST_PAUSE_WHEN_REDISTRIBUTING_STATE) {
			if (isFetching || isDisposing) {
				synchronized (partialPauseLock) {
					long now = System.currentTimeMillis();
					try {
						partialPauseLock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					log("Blocked " + (System.currentTimeMillis() - now) + " ms. Because of partial block during state distribution.");
				}
			}
		}
		K key = keyContext.getCurrentKey();
		if (sortedKeyGroupsInCharge == null && tmpKeyGroupsInCharge == null) {
			return subGroupedStateMapManager.getMap(key, keyGroupIndex);
		}

		try {
			if (isKeyGroupInList(keyGroupIndex, sortedKeyGroupsInCharge) || isKeyGroupInList(keyGroupIndex, tmpKeyGroupsInCharge)) {
				switch (subGroupedStateMapManager.getKeyGroupStatus(keyGroupIndex)) {
					case EMPTY:
						return activeFetchKeyState(keyGroupIndex, key, true);
//						if (isFetching) {
//							return activeFetchKeyState(keyGroupIndex, key, true);
//						} else {
//							log("Error when getMapForKeyGroup for keyGroupIndex: " + keyGroupIndex + ". \'isFetching\' is false.");
//							outputKeyGroups();
//							throw new UnknownError(); // This shouldn't happen
//						}
					case PARTIAL_FETCHED:
						// key group is in fetching stage:
						// 1. return the key bin state if it exists
						// 2. else, actively fetch the key bin
						StateMap<K, N, S> stateMap = subGroupedStateMapManager.getMap(key, keyGroupIndex);
						if (stateMap == null || subGroupedStateMapManager.isKeyBinBorrowed(stateMap)) {
							return activeFetchKeyState(keyGroupIndex, key, true);
						} else {
							return stateMap;
						}
					case FETCHED:
						// key group fetching completed:
						// 1. return the key bin state if it exists.
						// 	As the key group has already been fetched, the state mustn't be borrowed any more.
						// 2. else, create a new key bin
						return subGroupedStateMapManager.getMap(key, keyGroupIndex);
					default:
						throw new UnknownError(); // This shouldn't happen
				}
			} else {
				log("keyGroupIndex: " + keyGroupIndex + " is not in this operator's charge");
				outputKeyGroups();
				if (TEST_PAUSE_WHEN_REDISTRIBUTING_STATE) {
					return createStateMap();
				} else {
					throw new UnknownError(); // This shouldn't happen
				}
			}
		} catch (Exception e) {
			log("Error when getMapForKeyGroup.");
			e.printStackTrace();
			throw new UnknownError(); // This shouldn't happen
		}
	}

	private StateMap<K, N, S> activeFetchKeyState(int keyGroupIndex, K key, boolean deleteRemote) throws Exception {
		log("Actively fetching key: " + key + " in keyGroup: " + keyGroupIndex + ". at: " + System.currentTimeMillis());
//		log("send: " + System.currentTimeMillis());
		long now = System.currentTimeMillis();

		StateMap<K, N, S> resultMap;

//		rescalingResponder.askTaskToPostKeyBin(keyToBytes(keyGroupIndex, key, currentNamespace), keyGroupIndex, this.taskName);
//		log("Ask jobmanager: " + (System.currentTimeMillis() - now) + " ms.");
		postFetchRequestManager.publishRequest(keyGroupIndex, subGroupedStateMapManager.keyToBinHash(key), this.subtaskIndex);
		log("Ask postFetchRequestManager: " + (System.currentTimeMillis() - now) + " ms.");

		resultMap = fetchKeyBinState(key, currentNamespace, keyGroupIndex, deleteRemote);

		log("Used " + (System.currentTimeMillis() - now) + " ms.");
		return resultMap;
	}

	private CompletableFuture<Acknowledge> postDisposedStateMapAsync() {
		return postDisposedStateMapAsync(null);
	}

	private CompletableFuture<Acknowledge> postDisposedStateMapAsync(SubTaskMigrationInstruction instruction) {
		return CompletableFuture.supplyAsync(() -> {
			List<CompletableFuture> futures = new ArrayList<>();
			if (instruction == null) {
				for (KeyGroupRange keyGroupRange : sortedKeyGroupsToDispose) {
					for (int keyGroup = keyGroupRange.getStartKeyGroup(); keyGroup <= keyGroupRange.getEndKeyGroup(); keyGroup++) {
						try {
							// TODO: set batch size
//							futures.add(postStateMapFromPreviousStateMapsAsync(keyGroup));
							postKeyGroupState(keyGroup);
//							postKeyGroupStateInKeyBins(keyGroup);
						} catch (Exception e) {
							// todo: may fail here if the state has not been posted, handle it
							e.printStackTrace();
						}
					}
				}
			} else {
				for (int keyGroup : instruction.getKeyGroupsToDispose()) {
					try {
//						postKeyGroupState(keyGroup);
						postKeyGroupStateInKeyBins(keyGroup);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			try {
				if (futures.size() > 0) {
					CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
				}
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			return Acknowledge.get();
		}).whenComplete(
			((acknowledge, throwable) -> {
				finishPostStage(instruction);
			})
		);
	}

	private void postDisposedStateMapSync() {
		for (KeyGroupRange keyGroupRange : sortedKeyGroupsToDispose) {
			for (int keyGroup = keyGroupRange.getStartKeyGroup(); keyGroup <= keyGroupRange.getEndKeyGroup(); keyGroup++) {
				try {
					postKeyGroupState(keyGroup);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		finishPostStage(null);
	}

	private void finishPostStage(SubTaskMigrationInstruction instruction) {
		if (instruction == null) {
			isDisposing = false;
			sortedKeyGroupsToDispose.clear();
			toBeCleaned = true;
			log("Post stage completed");
		} else {
			log("Post stage completed: " + instruction);
		}
	}

	public void clean() {
		// TODO: call this
		subGroupedStateMapManager.clean(previousKeyGroupsInCharge);
		previousKeyGroupsInCharge = null;
		sortedKeyGroupsInCharge = copyOf(tmpKeyGroupsInCharge, true);
		toBeCleaned = false;
		postFetchRequestManager.stopWatch();
	}

	private CompletableFuture<Acknowledge> fetchStateMapAsync(SubTaskMigrationInstruction instruction) {
		return CompletableFuture.supplyAsync(() -> {
			fetchAllKeyGroups(instruction);

			return Acknowledge.get();
		}).whenComplete(
			((acknowledge, throwable) -> {
				finishFetchStage(instruction);
			})
		);
	}

	private CompletableFuture<Acknowledge> fetchStateMapSync() {
		long now = System.currentTimeMillis();
		// in this way, the operator instance is blocked until its state redistribution is completed
		fetchAllKeyGroups();

		finishFetchStage(null);
		log("Fetch took " + (System.currentTimeMillis() - now) + " ms.");
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	private void finishFetchStage(SubTaskMigrationInstruction instruction) {
		if (instruction == null) {
			isFetching = false;
			sortedKeyGroupsFetching.clear();
			log("Fetch completed.");
		} else {
			isFetching = false;
			log("Fetch completed: " + instruction);
		}
	}

	private void fetchAllKeyGroups() {
		fetchAllKeyGroups(null);
	}

	private void fetchAllKeyGroups(SubTaskMigrationInstruction instruction) {
		if (instruction == null) {
			for (KeyGroupRange keyGroupRange : sortedKeyGroupsFetching) {
				for (int keyGroup = keyGroupRange.getStartKeyGroup(); keyGroup <= keyGroupRange.getEndKeyGroup(); keyGroup++) {
					try {
						fetchKeyGroupStateMap(keyGroup, true);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		} else {
			for (int keyGroupIndex : instruction.getKeyGroupsToFetch()) {
				try {
					fetchKeyGroupStateMapInKeyBins(keyGroupIndex);
//					fetchKeyGroupStateMap(keyGroupIndex, true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		try {
			while (!fetchingKeyGroups.isEmpty()) {
				int keyGroup = fetchingKeyGroups.getLast();
				if (fetchKeyGroupStateMap(keyGroup, false)) {
					fetchingKeyGroups.removeLast();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void postKeyGroupState(int keyGroupIndex) throws Exception {

		byte[] data = subGroupedStateMapManager.keyGroupToBytes(keyGroupIndex);
		putInDb(keyGroupIndexToBytes(keyGroupIndex), data);
		log("\'POSTED\': keyGroupIndex: " + keyGroupIndex + ", bytes: " + data.length);
	}

	private void postKeyGroupStateInKeyBins(int keyGroupIndex) throws Exception {

		int dataLength = 0;
		byte[][] data = subGroupedStateMapManager.keyGroupToBytesInKeyBins(keyGroupIndex);
		for (int binHash = 0; binHash < data.length; binHash++) {
			putInDb(keyGroupAndBinHashToBytes(keyGroupIndex, binHash), data[binHash]);
			log("\'POSTED\', keyBin: " + binHash + "of key group: " + keyGroupIndex);
			for (byte b : keyGroupAndBinHashToBytes(keyGroupIndex, binHash)) {
				log("" + b);
			}
			dataLength += data[binHash].length;
		}
		log("\'POSTED\' (InKeyBins): keyGroupIndex: " + keyGroupIndex + ", bytes: " + dataLength);
	}

	private CompletableFuture postKeyGroupStateAsync(int keyGroupIndex) throws Exception {
		return CompletableFuture.supplyAsync(() -> {
			try {
				postKeyGroupState(keyGroupIndex);
			} catch (Exception e) {
				return FutureUtils.completedExceptionally(e);
			}
			return CompletableFuture.completedFuture(Acknowledge.get());
		});
	}

	private boolean fetchKeyGroupStateMap(int keyGroupIndex, boolean recordWhenFailed) throws Exception {
		byte[] value = null;
		int fetchTimes = 0;
		byte[] dbKey = keyGroupIndexToBytes(keyGroupIndex);
		while ((value = getFromDb(dbKey)) == null
				&& ((fetchTimes++) < MAX_FETCH_TIMES)) {
			Thread.sleep(FETCH_INTERVAL_MILLI);
		}

		if (value == null) {
			log("Failed to fetch keyGroupIndex: " + keyGroupIndex);
			if (recordWhenFailed) {
				fetchingKeyGroups.add(keyGroupIndex);
			}
			return false;
		}

		subGroupedStateMapManager.storeFetchedKeyGroupState(keyGroupIndex, value);
		log("\'FETCHED\': keyGroupIndex: " + keyGroupIndex +  ", bytes: " + value.length);
		return true;
	}

	private void fetchKeyGroupStateMapInKeyBins(int keyGroupIndex) throws Exception {
		int numBins = subGroupedStateMapManager.getNumBins();
		int dataLength = 0;
		LinkedList<Integer> unfetchedKeyBins = new LinkedList<>();
		for (int keyBinHash = 0; keyBinHash < numBins; keyBinHash++) {
			byte[] value = null;
			int fetchTimes = 0;
			byte[] dbKey = keyGroupAndBinHashToBytes(keyGroupIndex, keyBinHash);
			while (subGroupedStateMapManager.isKeyBinHashEmpty(keyGroupIndex, keyBinHash)
				&& (value = getFromDb(dbKey)) == null
				&& fetchTimes < MAX_FETCH_TIMES) {
				fetchTimes++;
			}
			if (value != null) {
				subGroupedStateMapManager.storeFetchedKeyBinHashState(keyGroupIndex, value, keyBinHash);
				dataLength += value.length;
			} else if (fetchTimes == MAX_FETCH_TIMES) {
				unfetchedKeyBins.addLast(keyBinHash);
			}
		}

		while (!unfetchedKeyBins.isEmpty()) {
			int keyBinHash = unfetchedKeyBins.removeLast();
			byte[] value = null;
			int fetchTimes = 0;
			byte[] dbKey = keyGroupAndBinHashToBytes(keyGroupIndex, keyBinHash);
			while (subGroupedStateMapManager.isKeyBinHashEmpty(keyGroupIndex, keyBinHash)
				&& (value = getFromDb(dbKey)) == null
				&& fetchTimes < MAX_FETCH_TIMES) {
				fetchTimes++;
			}
			if (value != null) {
				subGroupedStateMapManager.storeFetchedKeyBinHashState(keyGroupIndex, value, keyBinHash);
				dataLength += value.length;
			} else if (fetchTimes == MAX_FETCH_TIMES) {
				unfetchedKeyBins.addFirst(keyBinHash);
			}
		}

		log("\'FETCHED\': keyGroupIndex: " + keyGroupIndex +  ", bytes: " + dataLength);
	}

	private void log(String str) {
		// TODO: use log to output information
		if (OUTPUT_INFO) {
			System.out.println(this.subtaskIndex + "-" + str);
		}
	}

	private List<KeyGroupRange> copyOf(List<KeyGroupRange> source, boolean deepCopy) {
		if (source == null) {
			return null;
		}
		if (deepCopy) {
			return new ArrayList<>(source);
		} else {
			return source;
		}
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	@Nonnull
	@Override
	public PostFetchStateTableSnapshot<K, N, S> stateSnapshot() {
		return new PostFetchStateTableSnapshot<>(
			this,
			getKeySerializer(),
			getNamespaceSerializer(),
			getStateSerializer(),
			getMetaInfo().getStateSnapshotTransformFactory().createForDeserializedState().orElse(null));
	}

	/**
	 * This class encapsulates the snapshot logic.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 */
	static class PostFetchStateTableSnapshot<K, N, S>
		extends AbstractStateTableSnapshot<K, N, S> {

		PostFetchStateTableSnapshot(
			PostFetchStateTable<K, N, S> owningTable,
			TypeSerializer<K> localKeySerializer,
			TypeSerializer<N> localNamespaceSerializer,
			TypeSerializer<S> localStateSerializer,
			StateSnapshotTransformer<S> stateSnapshotTransformer) {
			super(owningTable,
				localKeySerializer,
				localNamespaceSerializer,
				localStateSerializer,
				stateSnapshotTransformer);
		}

		@Override
		protected StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> getStateMapSnapshotForKeyGroup(int keyGroup) {
			PostFetchStateMap<K, N, S> stateMap = (PostFetchStateMap<K, N, S>) owningStateTable.getMapForKeyGroup(keyGroup);

			return stateMap.stateSnapshot();
		}

		@Override
		public void release() {
		}
	}

	// TmpStateMapManager ----------------------------------------------------------------------------------------------------

	private StateMap<K, N, S> fetchKeyBinState(
		K key,
		N namespace,
		int keyGroupIndex,
		boolean deleteInRemote) throws Exception {

		long now = System.currentTimeMillis();

//		byte[] value = getKeyBinStateFromDb(keyGroupIndex, subGroupedStateMapManager.keyToBinHash(key));
//		log("1: " + (System.currentTimeMillis() - now));

//		while (subGroupedStateMapManager.isKeyBinEmpty(keyGroupIndex, key)
//			&& (value = getFromDb(dbKey)) == null) {
//			fetchTimes++;
//			Thread.sleep(FETCH_INTERVAL_MILLI);
////			log("fetchTimes: " + fetchTimes + ", " + (System.currentTimeMillis() - now));
//		}

		while (subGroupedStateMapManager.isKeyBinEmpty(keyGroupIndex, key)) {
			Thread.sleep(FETCH_INTERVAL_MILLI);
		}
//		if (deleteInRemote) {
//			dbClient.del(dbKey);
//		}
//		log("2: " + (System.currentTimeMillis() - now));
//		if (value != null) {
//			subGroupedStateMapManager.storeFetchedKeyBinState(keyGroupIndex, value, key);
//		}
		log("3: " + (System.currentTimeMillis() - now));
		return subGroupedStateMapManager.getMap(key, keyGroupIndex);
	}

	private byte[] getKeyBinStateFromDb(int keyGroupIndex, int keyBinHash) throws Exception {
		long now = System.currentTimeMillis();

		int fetchTimes = 0;
		byte[] dbKey = keyGroupAndBinHashToBytes(keyGroupIndex, keyBinHash);
		StringBuilder sb = new StringBuilder();
		byte[] value;
		while (true) {
			long beforeGet = System.currentTimeMillis();
			value = getFromDb(dbKey);
			sb.append(this.subtaskIndex).append("---").append(System.currentTimeMillis() - beforeGet).append("\n");
			if (!(subGroupedStateMapManager.isKeyBinHashEmpty(keyGroupIndex, keyBinHash)
				&& value == null)) {
				break;
			}
			fetchTimes++;
			Thread.sleep(FETCH_INTERVAL_MILLI);
		}

		log("Actively fetching keyGroup: " + keyGroupIndex + ". Completed after " + fetchTimes
			+ " times. Result: "
			+ (value == null ? "Fetched in regular process" : "Fetched in active process, value bytes: " + value.length));
		if (System.currentTimeMillis() - now > 100) {
			log(sb.toString());
		}
		return value;
	}

	private void postKeyBinState(byte[] keyData) {
		int keyGroupIndex;
		K key;
		try {
			KeyStruct keyStruct = bytesToKey(keyData);
			keyGroupIndex = keyStruct.keyGroupIndex;
			key = keyStruct.key;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		try {
			byte[] data;
//			synchronized (this) {
			long now = System.currentTimeMillis();
				data = subGroupedStateMapManager.keyBinToBytes(keyGroupIndex, key);
				if (System.currentTimeMillis() - now > 1000) {
					log("keyBinToBytes took too long: " + (System.currentTimeMillis() - now));
					long here1 = System.currentTimeMillis();
					byte[] tmp = subGroupedStateMapManager.keyBinToBytes(keyGroupIndex, key);
					long here2 = System.currentTimeMillis();
					System.out.println("2nd try took: " + (here2 - here1));
				}
				subGroupedStateMapManager.markKeyBinAsBorrowed(keyGroupIndex, key);
				log("markKeyBinAsBorrowed: " + key + ", keyGroupIndex: " + keyGroupIndex + ". At: " + System.currentTimeMillis());
//			}
			putInDb(keyData, data);
		} catch (Exception e) {
			e.printStackTrace();
			log("Error when postKeyBinState. key: " + key + "keyGroupIndex: " + keyGroupIndex);
		}
	}

	private byte[] keyToBytes(int keyGroupIndex, K key, N namespace) throws IOException {
		DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(12);
		dataOutputSerializer.writeInt(keyGroupIndex);
		keySerializer.serialize(key, dataOutputSerializer);
		getNamespaceSerializer().serialize(namespace, dataOutputSerializer);
		return dataOutputSerializer.getSharedBuffer();
	}

	private KeyStruct bytesToKey(byte[] data) throws IOException {
		DataInputDeserializer dataInputDeserializer = new DataInputDeserializer(data);
		int keyGroupIndex = dataInputDeserializer.readInt();
		K key = keySerializer.deserialize(dataInputDeserializer);
		N namespace = getNamespaceSerializer().deserialize(dataInputDeserializer);
		return new KeyStruct(keyGroupIndex, key, namespace);
	}

	void copyFromTmp() {
		throw new UnsupportedOperationException();
	}

	private class KeyStruct {
		int keyGroupIndex;
		K key;
		N namespace;

		KeyStruct(int keyGroupIndex, K key, N namespace) {
			this.keyGroupIndex = keyGroupIndex;
			this.key = key;
			this.namespace = namespace;
		}
	}

	private void checkInUseAndWait(int keyGroup, int keyBin) throws InterruptedException {
		while (subGroupedStateMapManager.isInUse(keyGroup, keyBin)) {
			synchronized (keyBinInUseLock) {
				keyBinInUseLock.wait();
			}
		}
	}

	void invokeByRequestManager(int fromSubTaskIndex, int keyGroup, int keyBin) {
		CompletableFuture.runAsync(() -> {
			try {
				long now = System.currentTimeMillis();
				checkInUseAndWait(keyGroup, keyBin);
				log("a: " + (System.currentTimeMillis() - now));
				byte[] data;
				subGroupedStateMapManager.setPosting(keyBin, keyGroup, currentNamespace);
				log("b: " + (System.currentTimeMillis() - now));
				data = subGroupedStateMapManager.keyBinHashToBytes(keyGroup, keyBin);
				log("c: " + (System.currentTimeMillis() - now));
				subGroupedStateMapManager.markKeyBinHashAsBorrowed(keyGroup, keyBin);
				log("d: " + (System.currentTimeMillis() - now));
				subGroupedStateMapManager.releasePosting();
				log("e: " + (System.currentTimeMillis() - now));
				synchronized (keyBinPostingLock) {
					keyBinPostingLock.notifyAll();
				}
				log("f: " + (System.currentTimeMillis() - now));
				log("markKeyBinAsBorrowed: " + keyBin + ", keyGroupIndex: " + keyGroup + ". At: " + System.currentTimeMillis());

				putInDb(keyGroupAndBinHashToBytes(keyGroup, keyBin), data);
				log("g: " + (System.currentTimeMillis() - now) + ": " + keyBin + ", keyGroupIndex: " + keyGroup);

				postFetchRequestManager.publishStateInfo(fromSubTaskIndex, keyGroup, keyBin);

//				postFetchRequestManager.publishState(fromSubTaskIndex, keyGroup, keyBin, data);

				log("h: " + (System.currentTimeMillis() - now) + ": " + keyBin + ", keyGroupIndex: " + keyGroup);
			} catch (Exception e) {
				e.printStackTrace();
				log("Error when postKeyBinState. keyBin: " + keyBin + "keyGroupIndex: " + keyGroup);
			}
		});
	}

	void onReceivingState(int keyGroup, int keyBinHash, byte[] stateData) {
		try {
			subGroupedStateMapManager.storeFetchedKeyBinHashState(keyGroup, stateData, keyBinHash);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	void onReceivingStateInfo(int keyGroup, int keyBinHash) {
		try {
			byte[] value = getKeyBinStateFromDb(keyGroup, keyBinHash);
			subGroupedStateMapManager.storeFetchedKeyBinHashState(keyGroup, value, keyBinHash);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void putInDb(byte[] key, byte[] data) throws Exception {
		// as if writing redis is costly, only for test
		Thread.sleep(TEST_REDIS_WRITING_COST_MILLI);

		dbClient.put(key, data);
	}

	private byte[] getFromDb(byte[] key) throws Exception {
		// as if reading redis is costly, only for test
		Thread.sleep(TEST_REDIS_READING_COST_MILLI);

		return dbClient.get(key);
	}
}
