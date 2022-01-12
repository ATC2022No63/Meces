package org.apache.flink.runtime.taskexecutor.rescale;

// 记录算子rescale进程
public class rescaleState {
    int channelToWait;

    public rescaleState(int nChannel) {
        channelToWait = nChannel;
    }

    public boolean onGetMsg() {
        return false;
    }
}
