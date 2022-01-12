package org.apache.flink.runtime.executiongraph;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.Serializable;

/**
 * A RescaleState indicates an object's state in rescaling process.
 * The object can be ExecutionVertex, ResultPartition, TaskDeploymentDescriptor, etc.
 * For any object in a normal process, the RescaleState is simply set to NONE.
 */
public enum RescaleState implements Serializable {
	NONE, MODIFIED, NEW, REMOVED, ALIGNING;

	private static final ConfigOption<RescaleState> configOption = ConfigOptions
		.key("execution.rescale.state")
		.enumType(RescaleState.class)
		.defaultValue(NONE)
		.withDescription("A RescaleState indicates an object's state in rescaling process.");

	public static ConfigOption<RescaleState> getConfigOption() {
		return configOption;
	}
}
