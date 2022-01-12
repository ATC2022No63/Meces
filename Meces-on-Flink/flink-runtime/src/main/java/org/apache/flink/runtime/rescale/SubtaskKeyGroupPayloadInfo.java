package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.executiongraph.RescaleState;
import org.apache.flink.runtime.state.KeyGroupRange;

import java.util.List;

public class SubtaskKeyGroupPayloadInfo extends RescalePayloadInfo {
	List<KeyGroupRange> keyGroupRanges;

	public SubtaskKeyGroupPayloadInfo(RescaleState state, List<KeyGroupRange> keyGroupRanges) {
		this.rescaleState = state;
		this.keyGroupRanges = keyGroupRanges;
	}
}
