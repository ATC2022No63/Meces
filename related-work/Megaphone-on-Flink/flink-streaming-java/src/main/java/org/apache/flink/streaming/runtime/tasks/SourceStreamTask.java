/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.microbatch.EndOfBatchEvent;
import org.apache.flink.runtime.microbatch.kafka.Pubber;
import org.apache.flink.runtime.microbatch.ArrayShuffle;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.flink.runtime.microbatch.BatchWatcher;
import org.apache.flink.runtime.microbatch.ProcessConf;

import java.util.Timer;

/**
 * {@link StreamTask} for executing a {@link StreamSource}.
 *
 * <p>One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the {@link SourceFunction} that it must only modify its state or emit elements in
 * a synchronized block that locks on the lock Object. Also, the modification of the state
 * and the emission of elements must happen in the same block of code that is protected by the
 * synchronized block.
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP>  Type of the stream source operator
 */

@Internal
public class SourceStreamTask<OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
        extends StreamTask<OUT, OP> {

    private final LegacySourceFunctionThread sourceThread;
    private final Object lock;

    private volatile boolean externallyInducedCheckpoints;
    private boolean enable;
    private BatchWatcher batchWatcher;
    private Timer timer;
    private int interval;
    private int migrationIndex = -1;
    private int nMagration;
    private Integer[] migrations;
    private int epoch;

    /**
     * Indicates whether this Task was purposefully finished (by finishTask()), in this case we
     * want to ignore exceptions thrown after finishing, to ensure shutdown works smoothly.
     */
    private volatile boolean isFinished = false;

    public SourceStreamTask(Environment env) throws Exception {
        this(env, new Object());
    }

    private SourceStreamTask(Environment env, Object lock) throws Exception {
        super(env, null, FatalExitExceptionHandler.INSTANCE, StreamTaskActionExecutor.synchronizedExecutor(lock));
        this.lock = Preconditions.checkNotNull(lock);
        this.sourceThread = new LegacySourceFunctionThread();
        String enable = ProcessConf.getProperty(ProcessConf.BATCH_ENABLED);
        this.enable = enable.equals("on");
    }

    @Override
    protected void init() {
        // we check if the source is actually inducing the checkpoints, rather
        // than the trigger
        SourceFunction<?> source = mainOperator.getUserFunction();
        if (source instanceof ExternallyInducedSource) {
            externallyInducedCheckpoints = true;

            ExternallyInducedSource.CheckpointTrigger triggerHook = new ExternallyInducedSource.CheckpointTrigger() {

                @Override
                public void triggerCheckpoint(long checkpointId) throws FlinkException {
                    // TODO - we need to see how to derive those. We should probably not encode this in the
                    // TODO -   source's trigger message, but do a handshake in this task between the trigger
                    // TODO -   message from the master, and the source's trigger notification
                    final CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation(
                            configuration.isExactlyOnceCheckpointMode(),
                            configuration.isUnalignedCheckpointsEnabled(),
                            configuration.getAlignmentTimeout());
                    final long timestamp = System.currentTimeMillis();

                    final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);

                    try {
                        SourceStreamTask.super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, false)
                                .get();
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new FlinkException(e.getMessage(), e);
                    }
                }
            };

            ((ExternallyInducedSource<?, ?>) source).setCheckpointTrigger(triggerHook);
        }
        getEnvironment().getMetricGroup().getIOMetricGroup().gauge(MetricNames.CHECKPOINT_START_DELAY_TIME, this::getAsyncCheckpointStartDelayNanos);
        if (enable) {
            Integer nSink = Integer.parseInt(ProcessConf.getProperty(ProcessConf.NUM_SINK));
            interval = Integer.parseInt(ProcessConf.getProperty(ProcessConf.BATCH_INTERVAL));
            int subindex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();
            batchWatcher = new BatchWatcher(subindex, nSink);
            timer = new Timer(true);
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    startBatch();
                }
            }, 2000);
        }
    }

    public void startBatch() {
        mailboxProcessor.getMainMailboxExecutor().execute(() -> {
            EndOfBatchEvent endOfBatchEvent = new EndOfBatchEvent();
            int index;
            if (migrationIndex == -1)
                index = -1;
            else
                index = migrations[migrationIndex++];
            endOfBatchEvent.setRescale(index);
            broadcastBatchSignal(endOfBatchEvent);
            String prefix = Pubber.BATCH_PREFIX;
            int watch = batchWatcher.watch(prefix);
            epoch++;
            if (migrationIndex == nMagration) {
                migrationIndex = -1;
                LOG.info("migration done with {} ", nMagration);
            }
            LOG.info("this epoch:{},get epoch:{}", epoch, watch);
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    startBatch();
                }
            }, interval);
        }, "emit signal");
    }

    public void triggerSignal() {
        nMagration = Integer.parseInt(ProcessConf.getProperty(ProcessConf.MAGRATION_NUM));
        int maxParallelism = 128;
        Integer[] tmp = new Integer[maxParallelism];
        for (int i = 0; i < maxParallelism; i++) {
            tmp[i] = i;
        }

        int seed = getEnvironment().getTaskInfo().getTaskName().hashCode();
        ArrayShuffle arrayShuffle = new ArrayShuffle(seed);
        arrayShuffle.shuffle(tmp);
        migrations = new Integer[nMagration];
        for (int i = 0; i < nMagration; i++) {
            migrations[i] = tmp[i];
        }
        StringBuilder sb = new StringBuilder();
        for (Integer migration : migrations) {
            sb.append(migration).append(" ");
        }
        LOG.info("migration order:{}", sb.toString());
        migrationIndex = 0;
    }

    @Override
    protected void advanceToEndOfEventTime() throws Exception {
        mainOperator.advanceToEndOfEventTime();
    }

    @Override
    protected void cleanup() {
        // does not hold any resources, so no cleanup needed
    }

    @Override
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {

        controller.suspendDefaultAction();

        // Against the usual contract of this method, this implementation is not step-wise but blocking instead for
        // compatibility reasons with the current source interface (source functions run as a loop, not in steps).
        sourceThread.setTaskDescription(getName());
        sourceThread.start();
        sourceThread.getCompletionFuture().whenComplete((Void ignore, Throwable sourceThreadThrowable) -> {
            if (isCanceled() && ExceptionUtils.findThrowable(sourceThreadThrowable, InterruptedException.class).isPresent()) {
                mailboxProcessor.reportThrowable(new CancelTaskException(sourceThreadThrowable));
            } else if (!isFinished && sourceThreadThrowable != null) {
                mailboxProcessor.reportThrowable(sourceThreadThrowable);
            } else {
                mailboxProcessor.allActionsCompleted();
            }
        });
    }

    @Override
    protected void cancelTask() {
        try {
            if (mainOperator != null) {
                mainOperator.cancel();
            }
        } finally {
            if (sourceThread.isAlive()) {
                sourceThread.interrupt();
            } else if (!sourceThread.getCompletionFuture().isDone()) {
                // source thread didn't start
                sourceThread.getCompletionFuture().complete(null);
            }
        }
    }

    @Override
    protected void finishTask() throws Exception {
        isFinished = true;
        cancelTask();
    }

    @Override
    protected CompletableFuture<Void> getCompletionFuture() {
        return sourceThread.getCompletionFuture();
    }

    // ------------------------------------------------------------------------
    //  Checkpointing
    // ------------------------------------------------------------------------

    @Override
    public Future<Boolean> triggerCheckpointAsync(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
        if (!externallyInducedCheckpoints) {
            return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);
        } else {
            // we do not trigger checkpoints here, we simply state whether we can trigger them
            synchronized (lock) {
                return CompletableFuture.completedFuture(isRunning());
            }
        }
    }

    @Override
    protected void declineCheckpoint(long checkpointId) {
        if (!externallyInducedCheckpoints) {
            super.declineCheckpoint(checkpointId);
        }
    }

    /**
     * Runnable that executes the the source function in the head operator.
     */
    private class LegacySourceFunctionThread extends Thread {

        private final CompletableFuture<Void> completionFuture;

        LegacySourceFunctionThread() {
            this.completionFuture = new CompletableFuture<>();
        }

        @Override
        public void run() {
            try {
                mainOperator.run(lock, getStreamStatusMaintainer(), operatorChain);
                completionFuture.complete(null);
            } catch (Throwable t) {
                // Note, t can be also an InterruptedException
                completionFuture.completeExceptionally(t);
            }
        }

        public void setTaskDescription(final String taskDescription) {
            setName("Legacy Source Thread - " + taskDescription);
        }

        /**
         * @return future that is completed once this thread completes. If this task {@link #isFailing()} and this thread
         * is not alive (e.g. not started) returns a normally completed future.
         */
        CompletableFuture<Void> getCompletionFuture() {
            return isFailing() && !isAlive() ? CompletableFuture.completedFuture(null) : completionFuture;
        }
    }
}
