package org.apache.flink.runtime.rescale;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * This coordinator coordinates the rescale signals.
 * Each execution graph has its RescaleCoordinator.
 * Each RescaleCoordinator monitors results of rescale actions in different rescaling stages.
 */
public class RescaleCoordinator {
	private static final Logger LOG = LoggerFactory.getLogger(RescaleCoordinator.class);
	private final Map<Long, RescaleResultCollector> rescaleResultCollectors = new HashMap<>();

	private ExecutionVertex[] tasksToTrigger;
	private ExecutionVertex[] tasksToWaitFor;
	private ExecutionVertex[] tasksToCommitTo;

	private Set<ExecutionAttemptID> rescalingTasks = new HashSet<>();
	CompletableFuture<Acknowledge> rescalingDeployingTasksFuture = new CompletableFuture<>();

	private final JobID job;

	public RescaleCoordinator(
		JobID job,
		ExecutionVertex[] tasksToTrigger,
		ExecutionVertex[] tasksToWaitFor,
		ExecutionVertex[] tasksToCommitTo) {

		this.job = job;
		this.tasksToTrigger = tasksToTrigger;
		this.tasksToWaitFor = tasksToWaitFor;
		this.tasksToCommitTo = tasksToCommitTo;
	}

//	public CompletableFuture<Boolean> triggeringRescaleSignal() {
//
//	}
	public void startTriggeringRescaleSignal(RescaleSignal rescaleSignal) {
		try {
			final Execution[] executions = getTriggerExecutions();
			for (Execution execution: executions) {
				execution.triggerRescaleSignal(rescaleSignal);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public CompletableFuture<Acknowledge> startNewRescaleAction(long rescaleSignalID, Set<ExecutionAttemptID> executionsToWaitFor) {
		RescaleResultCollector rescaleResultCollector = new RescaleResultCollector();
		this.rescaleResultCollectors.put(rescaleSignalID, rescaleResultCollector);
		return rescaleResultCollector.startNewRescaleAction(executionsToWaitFor, rescaleSignalID);
	}

	public void acknowledgeRescale(JobID jobID, ExecutionAttemptID executionAttemptID, long timestampAsID) {
		RescaleResultCollector rescaleResultCollector = rescaleResultCollectors.get(timestampAsID);
		if (rescaleResultCollector != null) {
			rescaleResultCollector.acknowledgeRescale(executionAttemptID, timestampAsID);
		}
	}

	public void acknowledgeDeploymentForRescaling(ExecutionAttemptID executionAttemptID) {
		notifyRescaleDeploymentCompleted(executionAttemptID);
	}

	private Execution[] getTriggerExecutions() throws CheckpointException {
		Execution[] executions = new Execution[tasksToTrigger.length];
		for (int i = 0; i < tasksToTrigger.length; i++) {
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if (ee == null) {
				LOG.info(
					"Rescale signal triggering task {} of job {} is not being executed at the moment. Aborting.",
					tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
					job);
				// TODO: Implement RescaleException instead
				throw new CheckpointException(
					CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			} else if (ee.getState() == ExecutionState.RUNNING) {
				executions[i] = ee;
			} else {
				LOG.info(
					"Rescale signal triggering task {} of job {} is not in state {} but {} instead. Aborting.",
					tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
					job,
					ExecutionState.RUNNING,
					ee.getState());
				throw new CheckpointException(
					CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
		}
		return executions;
	}

	public CompletableFuture<Acknowledge> notifyStartAsyncDeploymentForRescale(List<ExecutionVertex> executionVertices) {
		// TODO: one rescalingTasks and future for each rescaling action
		rescalingTasks.clear();
		for (ExecutionVertex executionVertex : executionVertices) {
			rescalingTasks.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
		}
		rescalingDeployingTasksFuture = new CompletableFuture<>();
		return rescalingDeployingTasksFuture;
	}

	public void notifyRescaleDeploymentCompleted(ExecutionAttemptID executionAttemptID) {
		rescalingTasks.remove(executionAttemptID);
		System.out.println(executionAttemptID);
		System.out.println(rescalingTasks.size());
		if (rescalingTasks.isEmpty()) {
			rescalingDeployingTasksFuture.complete(Acknowledge.get());
		}
	}

	private static class RescaleResultCollector {
		private Set<ExecutionAttemptID> executionsToWaitFor;
		private int numExecutionsToWaitFor = -1;
		private long rescaleActionID = -1;
		private CompletableFuture<Acknowledge> future;

		CompletableFuture<Acknowledge> startNewRescaleAction(Set<ExecutionAttemptID> executionsToWaitFor, long rescaleActionID) {
			this.executionsToWaitFor = executionsToWaitFor;
			this.numExecutionsToWaitFor = executionsToWaitFor.size();
			this.rescaleActionID = rescaleActionID;
			this.future = new CompletableFuture<>();
			return future;
		}

		void acknowledgeRescale(ExecutionAttemptID executionAttemptID, long timestampAsID) {
			synchronized (this) {
				if (timestampAsID == this.rescaleActionID) {
					if (executionsToWaitFor.contains(executionAttemptID)) {
						executionsToWaitFor.remove(executionAttemptID);
						numExecutionsToWaitFor--;
//						System.out.println("numExecutionsToWaitFor" + numExecutionsToWaitFor);
						if (numExecutionsToWaitFor <= 0) {
							future.complete(Acknowledge.get());
						}
					}
				}
			}
		}
	}
}
