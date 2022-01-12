package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.RescaleState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeDifferenceComputation;
import org.apache.flink.runtime.state.redis.ProcessLevelConf;
import org.apache.flink.runtime.state.rescale.JobMigrateInstruction;
import org.apache.flink.runtime.state.rescale.JobVertexMigrationInstruction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * StateDutyManager manages the allocation of states among operators.
 */
public class StateDutyManager {
	private Map<JobVertexID, JobVertexInfo> vertexKeyGroupMap = new HashMap<>();
	private Map<JobVertexID, JobVertexInfo> futureVertexKeyGroupMap;
	private Map<JobVertexID, MigratingJobVertexInfo> migratingJobVertexInfoMap = new HashMap<>();
	private int migratingSteps;
	private ExecutionGraph executionGraph;
	private final int batchSize = Integer.parseInt(ProcessLevelConf.getProperty(ProcessLevelConf.STATE_BATCH_SIZE));

	private static class JobVertexInfo {
		int[] routeTable;
		List<KeyGroupRange>[] operatorStates;
		int parallelism;

		JobVertexInfo(int maxParallelism, int parallelism) {
			this.routeTable = new int[maxParallelism];
			this.parallelism = parallelism;
		}
	}

	private static class MigratingJobVertexInfo {
		LinkedList<Integer>[] keyGroupsToDispose;
		int steps;

		MigratingJobVertexInfo() {
		}

		@SuppressWarnings("unchecked")
		void init(int parallelism) {
			keyGroupsToDispose = new LinkedList[parallelism];
		}

		void put(int subTaskIndex, List<KeyGroupRange> subtaskKeyGroupsToDispose) {
			this.keyGroupsToDispose[subTaskIndex] = KeyGroupRangeDifferenceComputation.keyGroupRangesToLinkedList(subtaskKeyGroupsToDispose);
		}
	}

	StateDutyManager(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;
	}

	public void init() {
		for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
			int parallelism = ejv.getParallelism();
			int maxParallelism = ejv.getMaxParallelism();
			allocateStateForVertexUsingModulusOperation(
				parallelism,
				maxParallelism,
				ejv,
				vertexKeyGroupMap);
		}
	}

	public int getRemainingMigratingSteps() {
		return migratingSteps;
	}

	JobMigrateInstruction migrateOneStep() {
		JobMigrateInstruction jobMigrateInstruction = new JobMigrateInstruction();
		executionGraph
			.getAllVertices()
			.values()
			.stream()
			.filter(ejv -> ejv.rescaleState == RescaleState.MODIFIED)
			.map(ExecutionJobVertex::getJobVertexId)
			.forEach(jobVertexID ->  {
				JobVertexMigrationInstruction jobVertexMigrationInstruction = migrateOneStep(jobVertexID);
				if (jobVertexMigrationInstruction != null) {
					jobMigrateInstruction.put(jobVertexID, jobVertexMigrationInstruction);
				}
			});

		migratingSteps--;
		return jobMigrateInstruction;
	}

	JobMigrateInstruction migrateOneBatch() {
		JobMigrateInstruction jobMigrateInstruction = new JobMigrateInstruction();
		executionGraph
			.getAllVertices()
			.values()
			.stream()
			.filter(ejv -> ejv.rescaleState == RescaleState.MODIFIED)
			.map(ExecutionJobVertex::getJobVertexId)
			.forEach(jobVertexID ->  {
				JobVertexMigrationInstruction jobVertexMigrationInstruction = migrateOneBatch(jobVertexID);
				if (jobVertexMigrationInstruction != null) {
					jobMigrateInstruction.put(jobVertexID, jobVertexMigrationInstruction);
				}
			});

		return jobMigrateInstruction.getMigratedVertexCount() > 0 ? jobMigrateInstruction : null;
	}

//	private int getOneKeyGroup(List<KeyGroupRange> keyGroupRanges) {
//		if (keyGroupRanges.size() == 0) {
//			return -1;
//		}
//		KeyGroupRange keyGroupRange = keyGroupRanges.get(0);
//		int target = keyGroupRange.getStartKeyGroup();
//		return
//	}

	private JobVertexMigrationInstruction migrateOneStep(JobVertexID jobVertexID) {
		JobVertexInfo future = futureVertexKeyGroupMap.get(jobVertexID);
		MigratingJobVertexInfo migrating = migratingJobVertexInfoMap.get(jobVertexID);

		if (migrating == null || migrating.steps <= 0) {
			return null;
		}

		int parallelism = migrating.keyGroupsToDispose.length;
		JobVertexMigrationInstruction instruction = new JobVertexMigrationInstruction(parallelism);
		for (int i = 0; i < parallelism; i++) {
			if (!migrating.keyGroupsToDispose[i].isEmpty()) {
				int targetKeyGroup = migrating.keyGroupsToDispose[i].removeFirst();
				int toSubTask = future.routeTable[targetKeyGroup];
				instruction.addDisposedKeyGroup(targetKeyGroup, i, toSubTask);
				instruction.addFetchingKeyGroup(targetKeyGroup, i, toSubTask);
			}
		}

		migrating.steps--;
		return instruction;
	}

	private JobVertexMigrationInstruction migrateOneBatch(JobVertexID jobVertexID) {
		JobVertexInfo future = futureVertexKeyGroupMap.get(jobVertexID);
		MigratingJobVertexInfo migrating = migratingJobVertexInfoMap.get(jobVertexID);

		if (migrating == null || migrating.steps <= 0) {
			return null;
		}

		int migratedKeyGroupCount = 0;
		int parallelism = migrating.keyGroupsToDispose.length;
		JobVertexMigrationInstruction instruction = new JobVertexMigrationInstruction(parallelism);
		for (int i = 0; i < parallelism; i++) {
			if (!migrating.keyGroupsToDispose[i].isEmpty() && migratedKeyGroupCount < batchSize) {
				int targetKeyGroup = migrating.keyGroupsToDispose[i].removeFirst();
				int toSubTask = future.routeTable[targetKeyGroup];
				instruction.addDisposedKeyGroup(targetKeyGroup, i, toSubTask);
				instruction.addFetchingKeyGroup(targetKeyGroup, i, toSubTask);
				migratedKeyGroupCount++;
			}
		}

		return migratedKeyGroupCount > 0 ? instruction : null;
	}

	public void update() {
		for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
			update(ejv.getJobVertexId(), ejv.getParallelism());
		}
	}

	public void update(JobVertexID jobVertexID, int newParallelism) {
		if (futureVertexKeyGroupMap == null) {
			futureVertexKeyGroupMap = new HashMap<>();
		}
		ExecutionJobVertex ejv = executionGraph.getAllVertices().get(jobVertexID);
		allocateStateForVertexUsingModulusOperation(
			newParallelism,
			ejv.getMaxParallelism(),
			ejv,
			futureVertexKeyGroupMap);
	}

	int prepareStateMigrationSteps() {
		int steps = 0;
		for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
			if (ejv.rescaleState == RescaleState.MODIFIED) {
				MigratingJobVertexInfo migratingJobVertexInfo = new MigratingJobVertexInfo();
				steps = Math.max(steps, prepareStateMigrationSteps(ejv.getJobVertexId(), migratingJobVertexInfo));
				this.migratingJobVertexInfoMap.put(ejv.getJobVertexId(), migratingJobVertexInfo);
			}
		}
		migratingSteps = steps;
		return steps;
	}

	private int prepareStateMigrationSteps(JobVertexID jobVertexID, MigratingJobVertexInfo migratingJobVertexInfo) {
		JobVertexInfo cur = vertexKeyGroupMap.get(jobVertexID);
		JobVertexInfo future = futureVertexKeyGroupMap.get(jobVertexID);

		int largerPara = Math.max(cur.parallelism, future.parallelism);
		migratingJobVertexInfo.init(largerPara);
		int maxSteps = 0;
		for (int i = 0; i < largerPara; i++) {
			List<KeyGroupRange> curStates = i >= cur.parallelism ? Collections.emptyList() : cur.operatorStates[i];
			List<KeyGroupRange> futureStates = i >= future.parallelism ? Collections.emptyList() : future.operatorStates[i];
			List<KeyGroupRange> keyGroupsToDispose = new ArrayList<>();
			List<KeyGroupRange> keyGroupsToFetch = new ArrayList<>();
			List<KeyGroupRange> keyGroupsRemained = new ArrayList<>();
			KeyGroupRangeDifferenceComputation.getDisposeAndFetching(curStates, futureStates, keyGroupsToDispose, keyGroupsToFetch, keyGroupsRemained);
			int numKeyGroupsToBeMigrated = getNumKeyGroups(keyGroupsToDispose);
			maxSteps = Math.max(maxSteps, numKeyGroupsToBeMigrated);
			migratingJobVertexInfo.put(i, keyGroupsToDispose);
		}
		migratingJobVertexInfo.steps = maxSteps;
		return maxSteps;
	}

	private int getNumKeyGroups(List<KeyGroupRange> keyGroupRanges) {
		int count = 0;
		for (KeyGroupRange keyGroupRange : keyGroupRanges) {
			count += keyGroupRange.getNumberOfKeyGroups();
		}
		return count;
	}

	@SuppressWarnings("unchecked")
	private void allocateStateForVertexUsingModulusOperation(
		int parallelism,
		int maxParallelism,
		ExecutionJobVertex ejv,
		Map<JobVertexID, JobVertexInfo> targetMap) {

		JobVertexInfo jobVertexInfo = new JobVertexInfo(maxParallelism, parallelism);
		jobVertexInfo.operatorStates = new List[parallelism];
		for (ExecutionVertex ev : ejv.getTaskVertices()) {
			int subTaskIndex = ev.getParallelSubtaskIndex();

			int startKeyGroup = (int) Math.ceil((double) subTaskIndex * maxParallelism / parallelism);
			int endKeyGroup = (int) Math.ceil((double) (subTaskIndex + 1) * maxParallelism / parallelism - 1);
			List<KeyGroupRange> keyGroupRanges = new ArrayList<>(Collections.singletonList(new KeyGroupRange(
				startKeyGroup,
				endKeyGroup
			)));
			jobVertexInfo.operatorStates[subTaskIndex] = keyGroupRanges;
			for (int i = startKeyGroup; i <= endKeyGroup; i++) {
				jobVertexInfo.routeTable[i] = subTaskIndex;
			}
		}
		targetMap.put(ejv.getJobVertexId(), jobVertexInfo);
	}

//	@SuppressWarnings("unchecked")
//	private void allocateStateForVertexUsingMinMigration(
//		int curParallelism,
//		int futureParallelism,
//		int maxParallelism,
//		ExecutionJobVertex ejv,
//		Map<JobVertexID, JobVertexInfo> currentMap,
//		Map<JobVertexID, JobVertexInfo> targetMap) {
//
//		JobVertexInfo jobVertexInfo = new JobVertexInfo(maxParallelism, futureParallelism);
//		jobVertexInfo.operatorStates = new List[futureParallelism];
//		for (ExecutionVertex ev : ejv.getTaskVertices()) {
//
//		}
//
//	}

	public void clean() {
		vertexKeyGroupMap.clear();
		migratingJobVertexInfoMap.clear();
		vertexKeyGroupMap = futureVertexKeyGroupMap;
		futureVertexKeyGroupMap = null;
	}
}
