package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;

public class RpcRescalingResponder {
	private final JobMasterGateway jobMasterGateway;

	private String taskName;

	public RpcRescalingResponder(JobMasterGateway jobMasterGateway) {
		this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
	}

	public void acknowledgeRescale(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		String taskName,
		long timestampAsID) {

		jobMasterGateway.acknowledgeRescale(
			jobID,
			executionAttemptID,
			taskName,
			timestampAsID);
	}

	public void acknowledgeDeploymentForRescaling(
		ExecutionAttemptID executionAttemptID) {

		jobMasterGateway.acknowledgeDeploymentForRescaling(
			executionAttemptID);
	}

	public void fetchKeyGroupFromTask(int keyGroupIndex, int requestingSubtaskIndex) {
		jobMasterGateway.fetchKeyGroupFromTask(keyGroupIndex, taskName, requestingSubtaskIndex);
	}

	public void fetchKeyGroupFromTask(int keyGroupIndex, int requestingSubtaskIndex, String taskName) {
		jobMasterGateway.fetchKeyGroupFromTask(keyGroupIndex, taskName, requestingSubtaskIndex);
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public void askTaskToPostKeyBin(byte[] keyBinData, int keyGroupIndex, String taskName) {
		jobMasterGateway.fetchKeyFromTask(keyBinData, keyGroupIndex, taskName);
	}
}
