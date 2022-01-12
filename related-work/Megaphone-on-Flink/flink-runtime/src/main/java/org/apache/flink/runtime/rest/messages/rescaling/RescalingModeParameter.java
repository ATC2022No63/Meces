package org.apache.flink.runtime.rest.messages.rescaling;

import org.apache.flink.runtime.rest.messages.MessagePathParameter;

/**
 * Rescale mode.
 */
public class RescalingModeParameter extends MessagePathParameter<Integer> {

	public static final String KEY = "rescaleMode";

	public RescalingModeParameter() {
		super(KEY);
	}

	@Override
	protected Integer convertFromString(String value) {
		return Integer.parseInt(value);
	}

	@Override
	protected String convertToString(Integer value) {
		return value.toString();
	}

	@Override
	public String getDescription() {
		return "Rescale mode.";
	}
}
