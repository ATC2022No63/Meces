package org.apache.flink.runtime.rest.messages.job.rescaling;

import org.apache.flink.runtime.rescale.RescaleSignal;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class RescaleTriggerRequestBody implements RequestBody {

	public static final String FIELD_NAME_RESCALE_MODE = "rescale-mode";
	public static final String FIELD_NAME_RESCALE_PARALLELISM = "rescale-parallelism";
	public static final String FIELD_NAME_RESCALE_PARALLELISM_LIST = "rescale-parallelism-list";

	@JsonProperty(FIELD_NAME_RESCALE_MODE)
	private final RescaleSignal.RescaleSignalType rescaleSignalType;

	@JsonProperty(FIELD_NAME_RESCALE_PARALLELISM)
	private final int globalParallelism;

	@JsonProperty(FIELD_NAME_RESCALE_PARALLELISM_LIST)
	private final Map<String, Integer> parallelismList;

	@JsonCreator
	public RescaleTriggerRequestBody(
		@JsonProperty(value = FIELD_NAME_RESCALE_MODE, defaultValue = "0") final RescaleSignal.RescaleSignalType rescaleSignalType,
		@JsonProperty(value = FIELD_NAME_RESCALE_PARALLELISM, defaultValue = "-1") final int globalParallelism,
		@JsonProperty(value = FIELD_NAME_RESCALE_PARALLELISM_LIST) final Map<String, Integer> parallelismList) {
		this.rescaleSignalType = rescaleSignalType;
		this.globalParallelism = globalParallelism;
		this.parallelismList = parallelismList;
	}

	public RescaleSignal.RescaleSignalType getRescaleSignalType() {
		return rescaleSignalType;
	}

	public int getGlobalParallelism() {
		return globalParallelism;
	}

	public Map<String, Integer> getParallelismList() {
		return parallelismList;
	}
}

