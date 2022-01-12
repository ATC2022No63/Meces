package org.apache.flink.runtime.state.rescale;

import java.io.Serializable;

public class JobVertexMigrationInstruction implements Serializable {
	private SubTaskMigrationInstruction[] instructions;

	public JobVertexMigrationInstruction(int parallelism) {
		this.instructions = new SubTaskMigrationInstruction[parallelism];
		for (int i = 0; i < parallelism; i++) {
			instructions[i] = new SubTaskMigrationInstruction();
		}
	}

	public void addDisposedKeyGroup(int keyGroup, int preOwnerIndex, int newOwnerIndex) {
		instructions[preOwnerIndex].addKeyGroupsToDispose(keyGroup, preOwnerIndex, newOwnerIndex);
	}

	public void addFetchingKeyGroup(int keyGroup, int preOwnerIndex, int newOwnerIndex) {
		instructions[newOwnerIndex].addKeyGroupsToFetch(keyGroup, preOwnerIndex, newOwnerIndex);
	}

	public SubTaskMigrationInstruction get(int subTaskIndex) {
		return instructions[subTaskIndex];
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < instructions.length; i++) {
			sb.append("\t").append(i).append(": ").append(instructions[i]).append("\n");
		}
		return sb.toString();
	}

	public SubTaskMigrationInstruction[] getInstructions() {
		return instructions;
	}
}
