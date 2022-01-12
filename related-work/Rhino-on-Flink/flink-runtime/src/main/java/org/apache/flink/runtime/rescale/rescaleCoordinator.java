package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class rescaleCoordinator {
    protected static final Logger log = LoggerFactory.getLogger(rescaleCoordinator.class);
    private ExecutionVertex[] toTrigger;

    enum phases {
        idle, prepare, rescaling, clean
    }

    public rescaleCoordinator(ExecutionVertex[] executions) {
        toTrigger = executions;
    }

    public void prepareRescaling(List<ExecutionVertex> allVertex, rescalePlan plan) {
        Set<TaskManagerGateway> tm = new HashSet<>();
        for (ExecutionVertex vertex : allVertex) {
            tm.add(vertex.getCurrentExecutionAttempt().getTaskmanager());
        }

        AtomicReference<Integer> cnt = new AtomicReference<>(tm.size());
        assert cnt.get() != 0 : "no task manager?";
        for (TaskManagerGateway taskManagerGateway : tm) {
            CompletableFuture.runAsync(() -> {
                taskManagerGateway.prepareForRescaling(plan);
                cnt.getAndSet(cnt.get() - 1);
                log.info("get ack from {}", taskManagerGateway.getAddress());
                if (cnt.get() == 0)
                    log.info("get ack from all!");
            });
        }
    }

    public void triggerSignal() {
        for (Execution execution : getTriggerManagers()) {
            execution.getTaskmanager().triggerSignal(execution.getAttemptId());
        }
    }

    private Execution[] getTriggerManagers() {
        Execution[] executions = new Execution[toTrigger.length];
        for (int i = 0; i < toTrigger.length; i++) {
            Execution ee = toTrigger[i].getCurrentExecutionAttempt();
            assert ee != null : "null execution";
            assert ee.getState() == ExecutionState.RUNNING : "not running but:" + ee.getState();
            executions[i] = ee;
        }
        return executions;
    }
}
