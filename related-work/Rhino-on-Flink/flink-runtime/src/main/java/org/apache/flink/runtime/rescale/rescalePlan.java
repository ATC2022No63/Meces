package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.Serializable;
import java.util.Map;


public class rescalePlan implements Serializable {
    Map<JobVertexID, Map<Integer, Integer>> plan;

    public rescalePlan(Map<JobVertexID, Map<Integer, Integer>> plan) {
        this.plan = plan;
    }

    public Map<JobVertexID, Map<Integer, Integer>> getPlan() {
        return plan;
    }
}
