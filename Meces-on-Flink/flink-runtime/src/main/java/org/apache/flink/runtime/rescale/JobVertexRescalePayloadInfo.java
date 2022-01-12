package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.executiongraph.RescaleState;
import org.apache.flink.runtime.state.rescale.JobVertexMigrationInstruction;
import org.apache.flink.runtime.state.rescale.SubTaskMigrationInstruction;

import java.util.HashMap;
import java.util.Map;

public class JobVertexRescalePayloadInfo extends RescalePayloadInfo {
	Map<Integer, Integer> routeTableDifference = new HashMap<>();

	JobVertexRescalePayloadInfo(RescaleState state) {
		this.rescaleState = state;
	}

	void setRouteTableDifference(JobVertexMigrationInstruction jobVertexMigrationInstruction) {
		SubTaskMigrationInstruction[] instructions = jobVertexMigrationInstruction.getInstructions();
		for (int i = 0; i < instructions.length; i++) {
			for (int keyGroupIndex : instructions[i].getKeyGroupsToFetch()) {
				routeTableDifference.put(keyGroupIndex, i);
			}
		}
	}
}
