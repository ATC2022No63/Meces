package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Objects;

/**
 * Partitioner selects the target channel based on the key group index and an inner routing table.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
public class RouteTableKeyGroupStreamPartitioner<T, K> extends StreamPartitioner<T> implements ConfigurableStreamPartitioner {
	private static final long serialVersionUID = 1L;

	private final KeySelector<T, K> keySelector;

	private int maxParallelism;
	private int[] routeTable;

	public RouteTableKeyGroupStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.maxParallelism = maxParallelism;
	}

	@Override
	public void setup(int numberOfChannels) {
		this.numberOfChannels = numberOfChannels;
		initiateRouTable();
	}

	@Override
	public boolean setup(int numberOfChannels, Map<Integer, Integer> routeTableDifference) {
		this.numberOfChannels = numberOfChannels;
		updatePartitionStrategy(routeTableDifference);
		return true;
	}

	private void initiateRouTable() {
		for (int i = 0; i < maxParallelism; i++) {
			this.routeTable[i] = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, numberOfChannels, i);
		}
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		K key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
		return routeTable[keyGroupIndex];
	}

	private void updatePartitionStrategy(Map<Integer, Integer> routeTableDifference) {
		routeTableDifference.forEach((keyGroupIndex, subtaskIndex) -> routeTable[keyGroupIndex] = subtaskIndex);
	}

	@Override
	public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
		return SubtaskStateMapper.RANGE;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "HASH";
	}

	@Override
	public void configure(int maxParallelism) {
		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		this.maxParallelism = maxParallelism;
		this.routeTable = new int[maxParallelism];
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		final RouteTableKeyGroupStreamPartitioner<?, ?> that = (RouteTableKeyGroupStreamPartitioner<?, ?>) o;
		return maxParallelism == that.maxParallelism &&
			keySelector.equals(that.keySelector);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), keySelector, maxParallelism);
	}
}
