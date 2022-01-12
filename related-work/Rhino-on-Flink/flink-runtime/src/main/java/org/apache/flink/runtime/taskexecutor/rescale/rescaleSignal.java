package org.apache.flink.runtime.taskexecutor.rescale;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.io.Serializable;

public class rescaleSignal extends RuntimeEvent implements Serializable {
    public rescaleSignal() {
    }

    @Override
    public void write(DataOutputView out) throws IOException {

    }

    @Override
    public void read(DataInputView in) throws IOException {

    }
}