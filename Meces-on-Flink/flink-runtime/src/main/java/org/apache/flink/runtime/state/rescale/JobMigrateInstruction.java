package org.apache.flink.runtime.state.rescale;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class JobMigrateInstruction implements Serializable {
	private Map<JobVertexID, JobVertexMigrationInstruction> instructionMap = new HashMap<>();
	private int migratedVertexCount = 0;

	public void put(JobVertexID jobVertexID, JobVertexMigrationInstruction jobVertexMigrationInstruction) {
		instructionMap.put(jobVertexID, jobVertexMigrationInstruction);
		migratedVertexCount++;
	}

	public JobVertexMigrationInstruction get(JobVertexID jobVertexID) {
		return instructionMap.get(jobVertexID);
	}

	public int getMigratedVertexCount() {
		return migratedVertexCount;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (JobVertexID jobVertexID : instructionMap.keySet()) {
			sb.append(jobVertexID).append(":\n");
			sb.append(instructionMap.get(jobVertexID)).append("\n");
		}
		return sb.toString();
	}
}
