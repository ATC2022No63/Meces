package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.executiongraph.RescaleState;
import org.apache.flink.runtime.state.rescale.SubTaskMigrationInstruction;

public class SubtaskMigrationPayloadInfo extends RescalePayloadInfo {
	SubTaskMigrationInstruction subTaskMigrationInstruction;

	SubtaskMigrationPayloadInfo(RescaleState state) {
		this.rescaleState = state;
	}

	SubtaskMigrationPayloadInfo(RescaleState state, SubTaskMigrationInstruction subTaskMigrationInstruction) {
		this.rescaleState = state;
		this.subTaskMigrationInstruction = subTaskMigrationInstruction;
	}
}
