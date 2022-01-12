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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWindowTableAggregate
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecGroupWindowTableAggregate
import org.apache.flink.table.planner.plan.utils.AggregateUtil.{isRowtimeAttribute, timeFieldIndex}
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy

import scala.collection.JavaConversions._

/**
  * Rule to convert a [[FlinkLogicalWindowTableAggregate]] into a
  * [[StreamExecGroupWindowTableAggregate]].
  */
class StreamExecGroupWindowTableAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalWindowTableAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecGroupWindowTableAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalWindowTableAggregate = call.rel(0)

    // check if we have grouping sets
    val groupSets = agg.getGroupSets.size() != 1 || agg.getGroupSets.get(0) != agg.getGroupSet
    if (groupSets) {
      throw new TableException("GROUPING SETS are currently not supported.")
    }

    true
  }

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[FlinkLogicalWindowTableAggregate]
    val input = agg.getInput
    val inputRowType = input.getRowType
    val cluster = rel.getCluster
    val requiredDistribution = if (agg.getGroupSet.cardinality() != 0) {
      FlinkRelDistribution.hash(agg.getGroupSet.asList)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    val requiredTraitSet = input.getTraitSet
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(input, requiredTraitSet)

    val timeField = agg.getWindow.timeAttribute
    val inputTimestampIndex = if (isRowtimeAttribute(timeField)) {
      timeFieldIndex(inputRowType, relBuilderFactory.create(cluster, null), timeField)
    } else {
      -1
    }

    val config = cluster.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val emitStrategy = WindowEmitStrategy(config, agg.getWindow)

    new StreamExecGroupWindowTableAggregate(
      cluster,
      providedTraitSet,
      newInput,
      rel.getRowType,
      inputRowType,
      agg.getGroupSet.toArray,
      agg.getAggCallList,
      agg.getWindow,
      agg.getNamedProperties,
      inputTimestampIndex,
      emitStrategy)
  }
}

object StreamExecGroupWindowTableAggregateRule {
  val INSTANCE: RelOptRule = new StreamExecGroupWindowTableAggregateRule
}


