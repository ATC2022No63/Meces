package org.apache.flink.runtime.microbatch;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

public class EndOfBatchEvent extends RuntimeEvent {

    /**
     * The singleton instance of this event.
     */
    public int rescaleIndex;
    public byte[] bytes;
    // ------------------------------------------------------------------------

    // not instantiable
    public EndOfBatchEvent() {
        bytes = new byte[0];
    }

    public void setRescale(int i) {
        rescaleIndex = i;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }
    // ------------------------------------------------------------------------

    @Override
    public void read(DataInputView in) throws IOException {
        // Nothing to do here
        this.rescaleIndex = in.readInt();
        int len = in.readInt();
        this.bytes = new byte[len];
        in.readFully(this.bytes);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        // Nothing to do here
        out.writeInt(rescaleIndex);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return 1965146672;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && obj.getClass() == EndOfBatchEvent.class;
    }

    @Override
    public String toString() {
        return Integer.toString(rescaleIndex);
    }
}
