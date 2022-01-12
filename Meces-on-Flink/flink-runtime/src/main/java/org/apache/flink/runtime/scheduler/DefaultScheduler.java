/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.RescaleState;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.restart.ThrowingRestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.ThrowingSlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.rescale.RescaleSignal;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.redis.ProcessLevelConf;
import org.apache.flink.runtime.state.redis.ShardedRedisClusterClient;
import org.apache.flink.runtime.state.rescale.JobMigrateInstruction;
import org.apache.flink.runtime.state.rescale.JobVertexMigrationInstruction;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.rescale.RescaleSignal.RescaleSignalType.ALL_IN_ONCE_FETCH;
import static org.apache.flink.runtime.rescale.RescaleSignal.RescaleSignalType.CLEAN;
import static org.apache.flink.runtime.rescale.RescaleSignal.RescaleSignalType.CREATE;
import static org.apache.flink.runtime.rescale.RescaleSignal.RescaleSignalType.GRADUAL_FETCH;
import static org.apache.flink.runtime.rescale.RescaleSignal.RescaleSignalType.PREPARE;
import static org.apache.flink.runtime.rescale.RescaleSignal.RescaleSignalType.UNSET;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The future default scheduler.
 */
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

	private final Logger log;

	private final ClassLoader userCodeLoader;

	private ExecutionSlotAllocator executionSlotAllocator;

	private final ExecutionFailureHandler executionFailureHandler;

	private final ScheduledExecutor delayExecutor;

	private SchedulingStrategy schedulingStrategy;

	private final ExecutionVertexOperations executionVertexOperations;

	private final Set<ExecutionVertexID> verticesWaitingForRestart;

	private final Consumer<ComponentMainThreadExecutor> startUpAction;

	private final SchedulingStrategyFactory schedulingStrategyFactory;
	private final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory;
	private final StateDutyManager stateDutyManager;

	DefaultScheduler(
		final Logger log,
		final JobGraph jobGraph,
		final BackPressureStatsTracker backPressureStatsTracker,
		final Executor ioExecutor,
		final Configuration jobMasterConfiguration,
		final Consumer<ComponentMainThreadExecutor> startUpAction,
		final ScheduledExecutorService futureExecutor,
		final ScheduledExecutor delayExecutor,
		final ClassLoader userCodeLoader,
		final CheckpointRecoveryFactory checkpointRecoveryFactory,
		final Time rpcTimeout,
		final BlobWriter blobWriter,
		final JobManagerJobMetricGroup jobManagerJobMetricGroup,
		final ShuffleMaster<?> shuffleMaster,
		final JobMasterPartitionTracker partitionTracker,
		final SchedulingStrategyFactory schedulingStrategyFactory,
		final FailoverStrategy.Factory failoverStrategyFactory,
		final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
		final ExecutionVertexOperations executionVertexOperations,
		final ExecutionVertexVersioner executionVertexVersioner,
		final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
		final ExecutionDeploymentTracker executionDeploymentTracker,
		long initializationTimestamp) throws Exception {

		super(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutor,
			jobMasterConfiguration,
			new ThrowingSlotProvider(), // this is not used any more in the new scheduler
			futureExecutor,
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			new ThrowingRestartStrategy.ThrowingRestartStrategyFactory(),
			blobWriter,
			jobManagerJobMetricGroup,
			Time.seconds(0), // this is not used any more in the new scheduler
			shuffleMaster,
			partitionTracker,
			executionVertexVersioner,
			executionDeploymentTracker,
			false,
			initializationTimestamp);

		this.log = log;

		this.delayExecutor = checkNotNull(delayExecutor);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.executionVertexOperations = checkNotNull(executionVertexOperations);

		final FailoverStrategy failoverStrategy = failoverStrategyFactory.create(
			getSchedulingTopology(),
			getResultPartitionAvailabilityChecker());
		log.info("Using failover strategy {} for {} ({}).", failoverStrategy, jobGraph.getName(), jobGraph.getJobID());

		this.executionFailureHandler = new ExecutionFailureHandler(
			getSchedulingTopology(),
			failoverStrategy,
			restartBackoffTimeStrategy);
		this.schedulingStrategyFactory = schedulingStrategyFactory;
		this.schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology());

		this.executionSlotAllocator = checkNotNull(executionSlotAllocatorFactory)
			.createInstance(new DefaultExecutionSlotAllocationContext());
		this.executionSlotAllocatorFactory = executionSlotAllocatorFactory;

		this.verticesWaitingForRestart = new HashSet<>();
		this.startUpAction = startUpAction;
		this.stateDutyManager = new StateDutyManager(this.executionGraph);
	}

	// ------------------------------------------------------------------------
	// SchedulerNG
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> rescale(int newGlobalParallelism, Map<String, Integer> parallelismList, RescaleSignal.RescaleSignalType rescaleSignalType) {
		log.info("Rescaling job {}, at stage {}.", this.getJobId(), rescaleSignalType);

		try {
			RescaleSignal rescaleSignal;
			CompletableFuture<Acknowledge> future;
			switch (rescaleSignalType) {
				case PREPARE:
					this.stateDutyManager.init();
					rescaleSignal = new RescaleSignal(PREPARE);
					future = getRescaleSignalHandleResultFuture(rescaleSignal);
					triggerRescaleSignalForPrepareStage(rescaleSignal);
					return future;
				case CREATE:
					checkNotNull(parallelismList);
					// modify execution graph
					this.executionGraph.modifyForRescale(newGlobalParallelism, parallelismList);
					this.stateDutyManager.update();

					// deploy the new execution graph
					super.schedulingTopology = executionGraph.getSchedulingTopology();
					this.schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology());
					ExecutionSlotAllocator newExecutionSlotAllocator = checkNotNull(executionSlotAllocatorFactory)
						.createInstance(new DefaultExecutionSlotAllocationContext());
					this.executionSlotAllocator.updateForRescale(newExecutionSlotAllocator);
					this.schedulingStrategy.startSchedulingForRescale();

					rescaleSignal = new RescaleSignal(CREATE);
					future = getRescaleSignalHandleResultFuture(rescaleSignal);
					triggerRescaleSignalForCreateStage(rescaleSignal);
					return future;
				case ALL_IN_ONCE_FETCH:
					rescaleSignal = new RescaleSignal(ALL_IN_ONCE_FETCH);
					future = getRescaleSignalHandleResultFuture(rescaleSignal);
					triggerRescaleSignalForAllInOnceFetchStage(rescaleSignal);
					return future;
				case GRADUAL_FETCH:
					return CompletableFuture.supplyAsync(this::gradualFetch);
				case CLEAN:
					this.schedulingStrategy.releaseResourcesForRescale();
					System.out.println(1);
					this.executionGraph.cleanRescaleStates();
					System.out.println(2);
					cleanRedis();
					System.out.println(3);
					this.stateDutyManager.clean();
					System.out.println(4);

					rescaleSignal = new RescaleSignal(CLEAN);
					future = getRescaleSignalHandleResultFuture(rescaleSignal);
					triggerRescaleSignalForCleanStage(rescaleSignal);
					return future;
				case ALL:
					return performAllStageInOrder(newGlobalParallelism, parallelismList);
				default:
					throw new UnsupportedOperationException();
			}
		} catch (Throwable e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	private Acknowledge gradualFetch() {

		int steps = this.stateDutyManager.prepareStateMigrationSteps();
		log.info("Starting gradual migration. Migration steps:" + steps);
//		log.info("Starting gradual migration. Migration steps.");
		int stepCounts = 0;
		while (stateDutyManager.getRemainingMigratingSteps() > 0) {
			JobMigrateInstruction jobMigrateInstruction = stateDutyManager.migrateOneBatch();
			if (jobMigrateInstruction == null) {
				break;
			}
			System.out.println(jobMigrateInstruction);
			RescaleSignal rescaleSignal = new RescaleSignal(GRADUAL_FETCH);
			CompletableFuture future = getRescaleSignalHandleResultFuture(rescaleSignal);
			triggerRescaleSignalForGradualFetchStage(rescaleSignal, jobMigrateInstruction);
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
//			log.info("Migration steps remain:" + stateDutyManager.getRemainingMigratingSteps());
			stepCounts++;
			log.info("Migration one batch: " + stepCounts);
		}
		return Acknowledge.get();
	}

	private void triggerRescaleSignalForPrepareStage(RescaleSignal rescaleSignal) {
		putRemovedVerticesInSignal(rescaleSignal);
		super.triggerRescaleSignal(rescaleSignal);
	}

	private void triggerRescaleSignalForCreateStage(RescaleSignal rescaleSignal) {
		for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
			int parallelism = ejv.getParallelism();
			int maxParallelism = ejv.getMaxParallelism();
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				int subTaskIndex = ev.getParallelSubtaskIndex();

				List<KeyGroupRange> keyGroupRanges = new ArrayList<>(Collections.singletonList(new KeyGroupRange(
					(int) Math.ceil((double) subTaskIndex * maxParallelism / parallelism),
					(int) Math.ceil((double) (subTaskIndex + 1) * maxParallelism / parallelism - 1)
				)));
				rescaleSignal.putOperator(ev.getTaskName(), ev.getParallelSubtaskIndex(), ev.rescaleState, keyGroupRanges);
			}
		}
		putRemovedVerticesInSignal(rescaleSignal);
		super.triggerRescaleSignal(rescaleSignal);
	}

	private void triggerRescaleSignalForAllInOnceFetchStage(RescaleSignal rescaleSignal) {
		for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				rescaleSignal.putOperator(ev.getTaskName(), ev.getParallelSubtaskIndex(), ev.rescaleState);
			}
			if (ejv.rescaleState == RescaleState.MODIFIED) {
				rescaleSignal.putJobVertex(ejv.getName(), ejv.rescaleState);
			}
		}
		putRemovedVerticesInSignal(rescaleSignal);
		super.triggerRescaleSignal(rescaleSignal);
	}

	private void triggerRescaleSignalForGradualFetchStage(RescaleSignal rescaleSignal, JobMigrateInstruction jobMigrateInstruction) {
		for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
			JobVertexMigrationInstruction jobVertexMigrationInstruction = jobMigrateInstruction.get(ejv.getJobVertexId());
			if (jobVertexMigrationInstruction != null) {
				// state migration info
				for (ExecutionVertex ev : ejv.getTaskVertices()) {
					rescaleSignal.putOperator(
						ev.getTaskName(),
						ev.getParallelSubtaskIndex(),
						ev.rescaleState,
						jobVertexMigrationInstruction.get(ev.getParallelSubtaskIndex()));
				}
				// route table info
				ejv.getInputs().forEach(intermediateResult ->
					rescaleSignal.setJobVertexRouteTableDifference(
						intermediateResult.getProducer().getName(),
						jobVertexMigrationInstruction,
						intermediateResult.getProducer().rescaleState));
			} else {
				for (ExecutionVertex ev : ejv.getTaskVertices()) {
					rescaleSignal.putOperator(ev.getTaskName(), ev.getParallelSubtaskIndex(), ev.rescaleState);
				}
			}

			if (ejv.rescaleState == RescaleState.MODIFIED) {
				rescaleSignal.putJobVertex(ejv.getName(), ejv.rescaleState);
			}
		}

		// removed sub tasks
		for (ExecutionVertex ev : executionGraph.removeExecutionVertices) {
			rescaleSignal.putOperator(
				ev.getTaskName(),
				ev.getParallelSubtaskIndex(),
				ev.rescaleState,
				jobMigrateInstruction.get(ev.getJobVertex().getJobVertexId()).get(ev.getParallelSubtaskIndex()));
		}
		super.triggerRescaleSignal(rescaleSignal);
	}

	private void triggerRescaleSignalForCleanStage(RescaleSignal rescaleSignal) {
		super.triggerRescaleSignal(rescaleSignal);
	}

	private CompletableFuture<Acknowledge> performAllStageInOrder(int newGlobalParallelism, Map<String, Integer> parallelismList) {
		if (Boolean.parseBoolean(ProcessLevelConf.getProperty(ProcessLevelConf.TEST_PARTIAL_PAUSE))) {
			return this.rescale(newGlobalParallelism, parallelismList, PREPARE)
				.thenComposeAsync(a -> this.rescale(newGlobalParallelism, parallelismList, CREATE))
				.thenComposeAsync(a -> this.rescale(newGlobalParallelism, parallelismList, ALL_IN_ONCE_FETCH))
				.thenComposeAsync(a -> this.rescale(newGlobalParallelism, parallelismList, CLEAN));
		} else {
			return this.rescale(newGlobalParallelism, parallelismList, PREPARE)
				.thenComposeAsync(a -> this.rescale(newGlobalParallelism, parallelismList, CREATE))
				.thenComposeAsync(a -> this.rescale(newGlobalParallelism, parallelismList, GRADUAL_FETCH))
				.thenComposeAsync(a -> this.rescale(newGlobalParallelism, parallelismList, CLEAN));
		}
//		return this.rescale(newGlobalParallelism, parallelismList, PREPARE)
//			.thenComposeAsync(a -> this.rescale(newGlobalParallelism, parallelismList, CREATE))
//			.thenComposeAsync(a -> this.rescale(newGlobalParallelism, parallelismList, FETCH));
	}

	private CompletableFuture<Acknowledge> getRescaleSignalHandleResultFuture(RescaleSignal rescaleSignal) {
		Set<ExecutionAttemptID> executionsToWaitFor = new HashSet<>();
		for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				if (ev.rescaleState != RescaleState.REMOVED) {
					executionsToWaitFor.add(ev.getCurrentExecutionAttempt().getAttemptId());
				}
			}
		}

		assert this.executionGraph.getRescaleCoordinator() != null;
		return this.executionGraph.getRescaleCoordinator().startNewRescaleAction(rescaleSignal.getTimeStampAsID(), executionsToWaitFor);
	}

	private void putRemovedVerticesInSignal(RescaleSignal rescaleSignal) {
		for (ExecutionVertex ev : executionGraph.removeExecutionVertices) {
			rescaleSignal.putOperator(ev.getTaskName(), ev.getParallelSubtaskIndex(), ev.rescaleState);
		}
	}

	private void cleanRedis() {
		try {
			ShardedRedisClusterClient.getProcessLevelClient().clearDB();
		} catch (Exception ignored) {
		}
	}

	@Override
	public void setMainThreadExecutor(ComponentMainThreadExecutor mainThreadExecutor) {
		super.setMainThreadExecutor(mainThreadExecutor);
		startUpAction.accept(mainThreadExecutor);
	}

	@Override
	protected long getNumberOfRestarts() {
		return executionFailureHandler.getNumberOfRestarts();
	}

	@Override
	protected void startSchedulingInternal() {
		log.info("Starting scheduling with scheduling strategy [{}]", schedulingStrategy.getClass().getName());
		prepareExecutionGraphForNgScheduling();
		schedulingStrategy.startScheduling();
	}

	@Override
	protected void updateTaskExecutionStateInternal(
			final ExecutionVertexID executionVertexId,
			final TaskExecutionStateTransition taskExecutionState) {

		schedulingStrategy.onExecutionStateChange(executionVertexId, taskExecutionState.getExecutionState());
		maybeHandleTaskFailure(taskExecutionState, executionVertexId);
	}

	private void maybeHandleTaskFailure(
			final TaskExecutionStateTransition taskExecutionState,
			final ExecutionVertexID executionVertexId) {

		if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
			final Throwable error = taskExecutionState.getError(userCodeLoader);
			handleTaskFailure(executionVertexId, error);
		}
	}

	private void handleTaskFailure(final ExecutionVertexID executionVertexId, @Nullable final Throwable error) {
		setGlobalFailureCause(error);
		notifyCoordinatorsAboutTaskFailure(executionVertexId, error);
		final FailureHandlingResult failureHandlingResult = executionFailureHandler.getFailureHandlingResult(executionVertexId, error);
		maybeRestartTasks(failureHandlingResult);
	}

	private void notifyCoordinatorsAboutTaskFailure(final ExecutionVertexID executionVertexId, @Nullable final Throwable error) {
		final ExecutionJobVertex jobVertex = getExecutionJobVertex(executionVertexId.getJobVertexId());
		final int subtaskIndex = executionVertexId.getSubtaskIndex();

		jobVertex.getOperatorCoordinators().forEach(c -> c.subtaskFailed(subtaskIndex, error));
	}

	@Override
	public void handleGlobalFailure(final Throwable error) {
		setGlobalFailureCause(error);

		log.info("Trying to recover from a global failure.", error);
		final FailureHandlingResult failureHandlingResult = executionFailureHandler.getGlobalFailureHandlingResult(error);
		maybeRestartTasks(failureHandlingResult);
	}

	private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
		if (failureHandlingResult.canRestart()) {
			restartTasksWithDelay(failureHandlingResult);
		} else {
			failJob(failureHandlingResult.getError());
		}
	}

	private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
		final Set<ExecutionVertexID> verticesToRestart = failureHandlingResult.getVerticesToRestart();

		final Set<ExecutionVertexVersion> executionVertexVersions =
			new HashSet<>(executionVertexVersioner.recordVertexModifications(verticesToRestart).values());
		final boolean globalRecovery = failureHandlingResult.isGlobalFailure();

		addVerticesToRestartPending(verticesToRestart);

		final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

		delayExecutor.schedule(
			() -> FutureUtils.assertNoException(
				cancelFuture.thenRunAsync(restartTasks(executionVertexVersions, globalRecovery), getMainThreadExecutor())),
			failureHandlingResult.getRestartDelayMS(),
			TimeUnit.MILLISECONDS);
	}

	private void addVerticesToRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
		verticesWaitingForRestart.addAll(verticesToRestart);
		transitionExecutionGraphState(JobStatus.RUNNING, JobStatus.RESTARTING);
	}

	private void removeVerticesFromRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
		verticesWaitingForRestart.removeAll(verticesToRestart);
		if (verticesWaitingForRestart.isEmpty()) {
			transitionExecutionGraphState(JobStatus.RESTARTING, JobStatus.RUNNING);
		}
	}

	private Runnable restartTasks(final Set<ExecutionVertexVersion> executionVertexVersions, final boolean isGlobalRecovery) {
		return () -> {
			final Set<ExecutionVertexID> verticesToRestart = executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

			removeVerticesFromRestartPending(verticesToRestart);

			resetForNewExecutions(verticesToRestart);

			try {
				restoreState(verticesToRestart, isGlobalRecovery);
			} catch (Throwable t) {
				handleGlobalFailure(t);
				return;
			}

			schedulingStrategy.restartTasks(verticesToRestart);
		};
	}

	private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
		final List<CompletableFuture<?>> cancelFutures = verticesToRestart.stream()
			.map(this::cancelExecutionVertex)
			.collect(Collectors.toList());

		return FutureUtils.combineAll(cancelFutures);
	}

	private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
		final ExecutionVertex vertex = getExecutionVertex(executionVertexId);

		notifyCoordinatorOfCancellation(vertex);

		executionSlotAllocator.cancel(executionVertexId);
		return executionVertexOperations.cancel(vertex);
	}

	@Override
	protected void scheduleOrUpdateConsumersInternal(final IntermediateResultPartitionID partitionId) {
		schedulingStrategy.onPartitionConsumable(partitionId);
	}

	// ------------------------------------------------------------------------
	// SchedulerOperations
	// ------------------------------------------------------------------------

	@Override
	public void allocateSlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		validateDeploymentOptions(executionVertexDeploymentOptions);

		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex =
			groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);

		final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.collect(Collectors.toList());

		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
			executionVertexVersioner.recordVertexModifications(verticesToDeploy);

		transitionToScheduled(verticesToDeploy);

		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			allocateSlots(executionVertexDeploymentOptions);

		final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(
			requiredVersionByVertex,
			deploymentOptionsByVertex,
			slotExecutionVertexAssignments);

		waitForAllSlotsAndDeploy(deploymentHandles);
	}

	@Override
	public void allocateSlotsAndDeployForRescale(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		allocateSlotsAndDeployForRescaleTopologically(executionVertexDeploymentOptions);
	}

	private void allocateSlotsAndDeployForRescaleTopologically(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		for (ExecutionVertexDeploymentOption evdp : executionVertexDeploymentOptions) {
			List<ExecutionVertexDeploymentOption> singleList = Collections.singletonList(evdp);
			if (getExecutionVertex(evdp.getExecutionVertexId()).rescaleState == RescaleState.NEW) {
				deployNewDeploymentsAndWaitUntilDone(singleList);
			} else {
				updateModifiedVertices(singleList);
			}
		}
	}

	private void allocateSlotsAndDeployForRescaleInBatch(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		List<ExecutionVertexDeploymentOption> newExecutionVertexDeploymentOptions = executionVertexDeploymentOptions
			.stream()
			.filter(executionVertexDeploymentOption ->
				getExecutionVertex(executionVertexDeploymentOption.getExecutionVertexId()).rescaleState == RescaleState.NEW)
			.collect(Collectors.toList());

		deployNewDeploymentsAndWaitUntilDone(newExecutionVertexDeploymentOptions);

		// we should first deploy the new ExecutionVertices, and then deploy the modified vertices.
		updateModifiedVertices(executionVertexDeploymentOptions);
	}

	private void deployNewDeploymentsAndWaitUntilDone(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex =
			groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);

		final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.collect(Collectors.toList());

		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
			executionVertexVersioner.recordVertexModifications(verticesToDeploy);

		transitionToScheduled(verticesToDeploy);

		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			allocateSlots(executionVertexDeploymentOptions);

		final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(
			requiredVersionByVertex,
			deploymentOptionsByVertex,
			slotExecutionVertexAssignments);

		waitForNewDeploymentsToBeDone(deploymentHandles);
	}

	private void updateModifiedVertices(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		updateModifiedVertices(executionVertexDeploymentOptions, UNSET);
	}

	private void updateModifiedVertices(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions, RescaleSignal.RescaleSignalType rescaleSignalType) {
		List<ExecutionVertex> deployingExecutionVertices = new ArrayList<>();

		executionVertexDeploymentOptions.forEach(executionVertexDeploymentOption -> {
			ExecutionVertex ev = getExecutionVertex(executionVertexDeploymentOption.getExecutionVertexId());
			if (ev.rescaleState == RescaleState.MODIFIED) {
				getExecutionVertex(executionVertexDeploymentOption.getExecutionVertexId())
					.getCurrentExecutionAttempt()
					.deployForRescale(rescaleSignalType);
				deployingExecutionVertices.add(ev);
			}
		});
		if (!deployingExecutionVertices.isEmpty()) {
			checkNotNull(executionGraph.getRescaleCoordinator());
			try {
				CompletableFuture<Acknowledge> future = executionGraph.getRescaleCoordinator().notifyStartAsyncDeploymentForRescale(deployingExecutionVertices);
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void cancelTasksAndChannelsForRescale(List<ExecutionVertexDeploymentOption> savedExecutionVertexDeploymentOptionsForRescale) {
		// cancel the removed vertices
		List<ExecutionVertex> removedExecutionVertices = this.executionGraph.removeExecutionVertices;
		int numRemovedExecutionVertices = removedExecutionVertices.size();
		CompletableFuture<?>[] cancelResultFutures = new CompletableFuture<?>[numRemovedExecutionVertices];
		for (int i = 0; i < numRemovedExecutionVertices; i++) {
			ExecutionVertex ev = removedExecutionVertices.get(i);
			CompletableFuture<Acknowledge> cancelResultFuture = new CompletableFuture<>();
			boolean sendRpcCallSuccess = ev.getCurrentExecutionAttempt().cancelAsync(cancelResultFuture);
			if (sendRpcCallSuccess) {
				cancelResultFutures[i] = cancelResultFuture;
			} else {
				log.warn("Cannot send cancel call of {}.", ev);
			}
		}

		// wait for the cancels to be completed
		CompletableFuture<Void> all = cancelResultFutures.length == 0 ? CompletableFuture.completedFuture(null) : CompletableFuture.allOf(cancelResultFutures);
//		all.join();
		all.whenCompleteAsync((v, throwable) -> {
			// cancel channels
			updateModifiedVertices(savedExecutionVertexDeploymentOptionsForRescale, CLEAN);
		});

	}

	@Override
	public void acknowledgeRescale(JobID jobID, ExecutionAttemptID executionAttemptID, long timestampAsID) {
		assert this.executionGraph.getRescaleCoordinator() != null;
		this.executionGraph.getRescaleCoordinator().acknowledgeRescale(jobID, executionAttemptID, timestampAsID);
//		for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
//			for (ExecutionVertex ev : ejv.getTaskVertices()) {
//				if (ev.getCurrentExecutionAttempt().getAttemptId().equals(executionAttemptID)) {
//					System.out.println("notifyJobMaster from " + ev.getTaskNameWithSubtaskIndex());
//				}
//			}
//		}
	}

	@Override
	public void acknowledgeDeploymentForRescaling(ExecutionAttemptID executionAttemptID) {
		assert this.executionGraph.getRescaleCoordinator() != null;
		this.executionGraph.getRescaleCoordinator().acknowledgeDeploymentForRescaling(executionAttemptID);
	}

	@Override
	public void fetchKeyGroupFromTask(int keyGroupIndex, String taskName, int requestingSubtaskIndex) {
		Objects.requireNonNull(getExecutionForKey(keyGroupIndex, taskName))
			.fetchKeyGroupFromTask(keyGroupIndex);
	}

	@Override
	public void fetchKeyFromTask(byte[] keyData, int keyGroupIndex, String taskName) {
		System.out.println("Get request: " + keyGroupIndex  + ", " + System.currentTimeMillis());
		Objects.requireNonNull(getExecutionForKey(keyGroupIndex, taskName))
			.fetchKeyFromTask(keyData, keyGroupIndex, taskName);
	}

	private Execution getExecutionForKey(int keyGroupIndex, String taskName) {
		try {
			for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
				if (ejv.getName().equals(taskName)) {
					int previousParallelism = ejv.previousParallelism;
					int maxParallelism = ejv.getMaxParallelism();
					int subTaskIndex = keyGroupIndex * previousParallelism / maxParallelism;
					return ejv.getTaskVertices()[subTaskIndex].getCurrentExecutionAttempt();

				}
			}
		} catch (Exception e) {
			return null;
		}
		return null;
	}

	private void validateDeploymentOptions(final Collection<ExecutionVertexDeploymentOption> deploymentOptions) {
		deploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(this::getExecutionVertex)
			.forEach(v -> checkState(
				v.getExecutionState() == ExecutionState.CREATED,
				"expected vertex %s to be in CREATED state, was: %s", v.getID(), v.getExecutionState()));
	}

	private static Map<ExecutionVertexID, ExecutionVertexDeploymentOption> groupDeploymentOptionsByVertexId(
			final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionVertexDeploymentOptions.stream().collect(Collectors.toMap(
				ExecutionVertexDeploymentOption::getExecutionVertexId,
				Function.identity()));
	}

	private List<SlotExecutionVertexAssignment> allocateSlots(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionSlotAllocator.allocateSlotsFor(executionVertexDeploymentOptions
			.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(this::getExecutionVertex)
			.map(ExecutionVertexSchedulingRequirementsMapper::from)
			.collect(Collectors.toList()));
	}

	private static List<DeploymentHandle> createDeploymentHandles(
		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex,
		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

		return slotExecutionVertexAssignments
			.stream()
			.map(slotExecutionVertexAssignment -> {
				final ExecutionVertexID executionVertexId = slotExecutionVertexAssignment.getExecutionVertexId();
				return new DeploymentHandle(
					requiredVersionByVertex.get(executionVertexId),
					deploymentOptionsByVertex.get(executionVertexId),
					slotExecutionVertexAssignment);
			})
			.collect(Collectors.toList());
	}

	private void waitForAllSlotsAndDeploy(final List<DeploymentHandle> deploymentHandles) {
		FutureUtils.assertNoException(
			assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles)));
	}

	private void waitForNewDeploymentsToBeDone(final List<DeploymentHandle> deploymentHandles) {
		CompletableFuture<Void> future = assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles));
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	private CompletableFuture<Void> assignAllResources(final List<DeploymentHandle> deploymentHandles) {
		final List<CompletableFuture<Void>> slotAssignedFutures = new ArrayList<>();
		for (DeploymentHandle deploymentHandle : deploymentHandles) {
			final CompletableFuture<Void> slotAssigned = deploymentHandle
				.getSlotExecutionVertexAssignment()
				.getLogicalSlotFuture()
				.handle(assignResourceOrHandleError(deploymentHandle));
			slotAssignedFutures.add(slotAssigned);
		}
		return FutureUtils.waitForAll(slotAssignedFutures);
	}

	private BiFunction<Void, Throwable, Void> deployAll(final List<DeploymentHandle> deploymentHandles) {
		return (ignored, throwable) -> {
			propagateIfNonNull(throwable);
			for (final DeploymentHandle deploymentHandle : deploymentHandles) {
				final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();
				final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
				checkState(slotAssigned.isDone());

				FutureUtils.assertNoException(
					slotAssigned.handle(deployOrHandleError(deploymentHandle)));
			}
			return null;
		};
	}

	private static void propagateIfNonNull(final Throwable throwable) {
		if (throwable != null) {
			throw new CompletionException(throwable);
		}
	}

	private BiFunction<LogicalSlot, Throwable, Void> assignResourceOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		final ExecutionVertexID executionVertexId = deploymentHandle.getExecutionVertexId();

		return (logicalSlot, throwable) -> {
			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				log.debug("Refusing to assign slot to execution vertex {} because this deployment was " +
					"superseded by another deployment", executionVertexId);
				releaseSlotIfPresent(logicalSlot);
				return null;
			}

			if (throwable == null) {
				final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
				final boolean sendScheduleOrUpdateConsumerMessage = deploymentHandle.getDeploymentOption().sendScheduleOrUpdateConsumerMessage();
				executionVertex
					.getCurrentExecutionAttempt()
					.registerProducedPartitions(logicalSlot.getTaskManagerLocation(), sendScheduleOrUpdateConsumerMessage);
				executionVertex.tryAssignResource(logicalSlot);
			} else {
				handleTaskDeploymentFailure(executionVertexId, maybeWrapWithNoResourceAvailableException(throwable));
			}
			return null;
		};
	}

	private void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
		if (logicalSlot != null) {
			logicalSlot.releaseSlot(null);
		}
	}

	private void handleTaskDeploymentFailure(final ExecutionVertexID executionVertexId, final Throwable error) {
		executionVertexOperations.markFailed(getExecutionVertex(executionVertexId), error);
	}

	private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
		final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
		if (strippedThrowable instanceof TimeoutException) {
			return new NoResourceAvailableException("Could not allocate the required slot within slot request timeout. " +
				"Please make sure that the cluster has enough resources.", failure);
		} else {
			return failure;
		}
	}

	private BiFunction<Object, Throwable, Void> deployOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();

		return (ignored, throwable) -> {
			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				log.debug("Refusing to deploy execution vertex {} because this deployment was " +
					"superseded by another deployment", executionVertexId);
				return null;
			}

			if (throwable == null) {
				deployTaskSafe(executionVertexId);
			} else {
				handleTaskDeploymentFailure(executionVertexId, throwable);
			}
			return null;
		};
	}

	private void deployTaskSafe(final ExecutionVertexID executionVertexId) {
		try {
			final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
			executionVertexOperations.deploy(executionVertex);
		} catch (Throwable e) {
			handleTaskDeploymentFailure(executionVertexId, e);
		}
	}

	private void notifyCoordinatorOfCancellation(ExecutionVertex vertex) {
		// this method makes a best effort to filter out duplicate notifications, meaning cases where
		// the coordinator was already notified for that specific task
		// we don't notify if the task is already FAILED, CANCELLING, or CANCELED

		final ExecutionState currentState = vertex.getExecutionState();
		if (currentState == ExecutionState.FAILED ||
				currentState == ExecutionState.CANCELING ||
				currentState == ExecutionState.CANCELED) {
			return;
		}

		for (OperatorCoordinator coordinator : vertex.getJobVertex().getOperatorCoordinators()) {
			coordinator.subtaskFailed(vertex.getParallelSubtaskIndex(), null);
		}
	}

	private class DefaultExecutionSlotAllocationContext implements ExecutionSlotAllocationContext {

		@Override
		public ResourceProfile getResourceProfile(final ExecutionVertexID executionVertexId) {
			return getExecutionVertex(executionVertexId).getResourceProfile();
		}

		@Override
		public AllocationID getPriorAllocationId(final ExecutionVertexID executionVertexId) {
			return getExecutionVertex(executionVertexId).getLatestPriorAllocation();
		}

		@Override
		public SchedulingTopology getSchedulingTopology() {
			return DefaultScheduler.this.getSchedulingTopology();
		}

		@Override
		public Set<SlotSharingGroup> getLogicalSlotSharingGroups() {
			return getJobGraph().getSlotSharingGroups();
		}

		@Override
		public Set<CoLocationGroupDesc> getCoLocationGroups() {
			return getJobGraph().getCoLocationGroupDescriptors();
		}

		@Override
		public Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(
				ExecutionVertexID executionVertexId) {
			return inputsLocationsRetriever.getConsumedResultPartitionsProducers(executionVertexId);
		}

		@Override
		public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
				ExecutionVertexID executionVertexId) {
			return inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);
		}

		@Override
		public Optional<TaskManagerLocation> getStateLocation(ExecutionVertexID executionVertexId) {
			return stateLocationRetriever.getStateLocation(executionVertexId);
		}
	}
}
