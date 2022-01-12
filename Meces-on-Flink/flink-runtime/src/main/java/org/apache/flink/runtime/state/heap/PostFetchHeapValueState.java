/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.executiongraph.RescaleState;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.rescale.SubTaskMigrationInstruction;
import org.apache.flink.runtime.taskexecutor.rpc.RpcRescalingResponder;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Heap-backed partitioned {@link ValueState} that is snapshotted into files and can be posted/fetched.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class PostFetchHeapValueState<K, N, V>
	extends AbstractHeapState<K, N, V>
	implements InternalValueState<K, N, V> {
	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable          The state table for which this state is associated to.
	 * @param keySerializer       The serializer for the keys.
	 * @param valueSerializer     The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue        The default value for the state.
	 */
	PostFetchHeapValueState(
		StateTable<K, N, V> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		V defaultValue) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);

		((PostFetchStateTable) stateTable).outputKeyGroups();
	}

	@Override
	public V value() {
//		System.out.println("get value");
//		System.out.printf("key, keyGroupIndex, namespace: %s, %s, %s\n", stateTable.keyContext.getCurrentKey(), stateTable.keyContext.getCurrentKeyGroupIndex(), currentNamespace);

		final V result = stateTable.get(currentNamespace);

		if (result == null) {
			return getDefaultValue();
		}

		return result;
	}

	@Override
	public void update(V value) {
//		System.out.println("update value" + value);

		if (value == null) {
			clear();
			return;
		}

		stateTable.put(currentNamespace, value);
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		StateTable<K, N, SV> stateTable,
		TypeSerializer<K> keySerializer) {
		return (IS) new PostFetchHeapValueState<>(
			stateTable,
			keySerializer,
			stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			stateDesc.getDefaultValue());
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public void markStateKeyGroups(
		List<KeyGroupRange> keyGroupsInCharge,
		RescaleState rescaleState,
		RpcRescalingResponder rescalingResponder,
		int subtaskIndex,
		String taskName) {
		System.out.println("setStateKeyGroupsInCharge: " + subtaskIndex);
		((PostFetchStateTable) this.stateTable).markStateKeyGroups(
			keyGroupsInCharge,
			rescaleState,
			rescalingResponder,
			subtaskIndex,
			taskName);
	}

	@Override
	public CompletableFuture enableMarkedStateKeyGroups(
		RescaleState rescaleState,
		SubTaskMigrationInstruction instruction) {
		System.out.println("enableMarkedStateKeyGroups");
		return ((PostFetchStateTable) this.stateTable).enableMarkedStateKeyGroups(rescaleState, instruction);
	}

	@Override
	public byte[] fetchKeyGroupFromTask(int keyGroupIndex) {
		System.out.println("fetchKeyGroupFromTask");
		return ((PostFetchStateTable) this.stateTable).activelyPostKeyGroupState(keyGroupIndex);
	}

	@Override
	public void fetchKeyFromTask(byte[] keyData, int keyGroupIndex) {
		System.out.println("fetchKeyFromTask");
		((PostFetchStateTable) this.stateTable).activelyPostKeyBinState(keyData);
	}
}
