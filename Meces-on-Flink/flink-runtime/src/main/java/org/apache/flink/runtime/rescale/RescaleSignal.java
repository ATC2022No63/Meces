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

package org.apache.flink.runtime.rescale;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.executiongraph.RescaleState;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.rescale.JobVertexMigrationInstruction;
import org.apache.flink.runtime.state.rescale.SubTaskMigrationInstruction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to send rescale signal.
 */
public class RescaleSignal extends RuntimeEvent implements Serializable {

	/**
	 * Used to differentiate rescale signal.
	 * CREATE: create new instances and channels
	 * FETCH: update partition and start new instances
	 * CLEAN: clean up channels and cancel removed instances
	 * ALL: perform all actions in order
	 */
	public enum RescaleSignalType {
		UNSET, SCALE, CONNECT, PREPARE, CREATE, ALL_IN_ONCE_FETCH, GRADUAL_FETCH, CLEAN, ALL;

		public boolean canTransferTo(RescaleSignalType target) {
			return (this == UNSET) ||
				(this == PREPARE && target == CREATE) ||
				(this == CREATE && target == ALL_IN_ONCE_FETCH) ||
				(this == CREATE && target == GRADUAL_FETCH) ||
				(this == GRADUAL_FETCH && target == GRADUAL_FETCH) ||
				(this == ALL_IN_ONCE_FETCH && target == CLEAN) ||
				(this == GRADUAL_FETCH && target == CLEAN) ||
				(this == CLEAN && target == PREPARE);
		}
	}

	private RescaleSignalType type;
	private Payload payload = new Payload();

	private long timeStampAsID = System.currentTimeMillis();

	// public nullary constructor for EventSerializer
	public RescaleSignal() {
		this.type = RescaleSignalType.UNSET;
	}

	public RescaleSignal(RescaleSignalType type) {
		this.type = type;
	}

	public RescaleSignalType getType() {
		return type;
	}

	public long getTimeStampAsID() {
		return timeStampAsID;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(type.ordinal());
		out.writeLong(timeStampAsID);
		byte[] payloadData = payload.toBytes();
		out.writeInt(payloadData.length);
		out.write(payloadData);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.type = RescaleSignalType.values()[in.readInt()];
		this.timeStampAsID = in.readLong();
		int payloadDataLength = in.readInt();
		byte[] payloadData = new byte[payloadDataLength];
		in.read(payloadData);
		try {
			this.payload = Payload.fromBytes(payloadData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void putOperator(String name, int subtaskIndex, RescaleState state) {
		payload.operatorMap.put(name + subtaskIndex, new SubtaskMigrationPayloadInfo(state, null));
	}

	public void putOperator(String name, int subtaskIndex, RescaleState state, SubTaskMigrationInstruction subTaskMigrationInstruction) {
		payload.operatorMap.put(name + subtaskIndex, new SubtaskMigrationPayloadInfo(state, subTaskMigrationInstruction));
	}

	public void putOperator(String name, int subtaskIndex, RescaleState state, List<KeyGroupRange> keyGroupRanges) {
		payload.operatorMap.put(name + subtaskIndex, new SubtaskKeyGroupPayloadInfo(state, keyGroupRanges));
	}

	public void putJobVertex(String name, RescaleState state) {
		payload.operatorMap.put(name, new JobVertexRescalePayloadInfo(state));
	}

	public void setJobVertexRouteTableDifference(
		String name,
		JobVertexMigrationInstruction jobVertexMigrationInstruction,
		RescaleState rescaleState) {
		JobVertexRescalePayloadInfo jobVertexRescalePayloadInfo;

		jobVertexRescalePayloadInfo = (JobVertexRescalePayloadInfo) payload.operatorMap.get(name);
		if (jobVertexRescalePayloadInfo == null) {
			jobVertexRescalePayloadInfo = new JobVertexRescalePayloadInfo(rescaleState);
			payload.operatorMap.put(name, jobVertexRescalePayloadInfo);
		}

		jobVertexRescalePayloadInfo.setRouteTableDifference(jobVertexMigrationInstruction);
	}

	public List<KeyGroupRange> getOperatorKeyRange(String taskName, int subtaskIndex) {
		return ((SubtaskKeyGroupPayloadInfo) payload.operatorMap.get(taskName + subtaskIndex)).keyGroupRanges;
	}

	public RescaleState getJobVertexRescaleState(String name) {
		try {
			RescaleState rs = payload.operatorMap.get(name).rescaleState;
			return rs == null ? RescaleState.NONE : rs;
		} catch (Exception e) {
			return RescaleState.NONE;
		}
	}

	public Map<Integer, Integer> getRouteTableDifference(String name) {
		try {
			return ((JobVertexRescalePayloadInfo) payload.operatorMap.get(name)).routeTableDifference;
		} catch (Exception e) {
			return Collections.emptyMap();
		}
	}

	public RescaleState getOperatorRescaleState(String name, int subtaskIndex) {
		try {
			RescaleState rs = payload.operatorMap.get(name + subtaskIndex).rescaleState;
			return rs == null ? RescaleState.NONE : rs;
		} catch (Exception e) {
			return RescaleState.NONE;
		}
	}

	public SubTaskMigrationInstruction getOperatorInstruction(String name, int subtaskIndex) {
		return ((SubtaskMigrationPayloadInfo) payload.operatorMap.get(name + subtaskIndex)).subTaskMigrationInstruction;
	}

	@Override
	public String toString() {
		return "RescaleSignal: " + type.toString();
	}

	public static class RescaleSignalHeader implements Serializable {
		private int senderParallelism;
		private int senderIndex;

		public RescaleSignalHeader(int senderParallelism, int senderIndex) {
			this.senderParallelism = senderParallelism;
			this.senderIndex = senderIndex;
		}

		public int getSenderParallelism() {
			return senderParallelism;
		}

		public int getSenderIndex() {
			return senderIndex;
		}

		public byte[] toBytes() throws IOException {
			ByteArrayOutputStream bis = new ByteArrayOutputStream();
			ObjectOutputStream ois = new ObjectOutputStream(bis);
			ois.writeObject(this);

			ois.close();
			bis.close();
			return bis.toByteArray();
		}

		public static RescaleSignalHeader fromBytes(byte[] data) throws IOException, ClassNotFoundException {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInputStream ois = new ObjectInputStream (bis);
			Object obj = ois.readObject();
			ois.close();
			bis.close();
			return (RescaleSignalHeader) obj;
		}
	}

	public static class Payload implements Serializable {
		Map<String, RescalePayloadInfo> operatorMap = new HashMap<>();

		public Payload() {
		}

		public byte[] toBytes() throws IOException {
			ByteArrayOutputStream bis = new ByteArrayOutputStream();
			ObjectOutputStream ois = new ObjectOutputStream(bis);
			ois.writeObject(this);

			ois.close();
			bis.close();
			return bis.toByteArray();
		}

		public static Payload fromBytes(byte[] data) throws IOException, ClassNotFoundException {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInputStream ois = new ObjectInputStream (bis);
			Object obj = ois.readObject();
			ois.close();
			bis.close();
			return (Payload) obj;
		}
	}
}
