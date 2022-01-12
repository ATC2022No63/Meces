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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.executiongraph.RescaleState;
import org.apache.flink.runtime.state.rescale.SubTaskMigrationInstruction;
import org.apache.flink.runtime.taskexecutor.rpc.RpcRescalingResponder;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface that combines both, the {@link KeyedStateBackend} interface, which encapsulates methods
 * responsible for keyed state management and the {@link SnapshotStrategy} which tells the system
 * how to snapshot the underlying state.
 *
 * <p><b>NOTE:</b> State backends that need to be notified of completed checkpoints can additionally implement
 * the {@link CheckpointListener} interface.
 *
 * @param <K> Type of the key by which state is keyed.
 */
public interface CheckpointableKeyedStateBackend<K> extends
		KeyedStateBackend<K>,
		SnapshotStrategy<SnapshotResult<KeyedStateHandle>>,
		Closeable {

	/**
	 * Returns the key groups which this state backend is responsible for.
	 */
	KeyGroupRange getKeyGroupRange();

	default void markStateKeyGroups(
		List<KeyGroupRange> keyGroupsInCharge,
		RescaleState rescaleState,
		RpcRescalingResponder rescalingResponder,
		int subtaskIndex,
		String taskName) {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture enableMarkedStateKeyGroups(
		RescaleState rescaleState,
		SubTaskMigrationInstruction instruction) {
		throw new UnsupportedOperationException();
	}

	default byte[] fetchKeyGroupFromTask(int keyGroupIndex) {
		throw new UnsupportedOperationException();
	}

	default void fetchKeyFromTask(byte[] keyData, int keyGroupIndex) {
		throw new UnsupportedOperationException();
	}
}
