package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.executiongraph.RescaleState;

import java.io.Serializable;

public abstract class RescalePayloadInfo implements Serializable {
	RescaleState rescaleState;
}
