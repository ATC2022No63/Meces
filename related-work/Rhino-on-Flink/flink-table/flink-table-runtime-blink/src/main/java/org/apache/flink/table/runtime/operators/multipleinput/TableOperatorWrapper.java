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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class handles the close, endInput and other related logic of a {@link StreamOperator}. It
 * also automatically propagates the end-input operation to the next wrapper that the {@link
 * #outputEdges} points to, so we only need to call the head wrapper's {@link
 * #endOperatorInput(int)} method.
 */
public class TableOperatorWrapper<OP extends StreamOperator<RowData>> implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The factory to create the wrapped operator. */
    private final StreamOperatorFactory<RowData> factory;

    /** the operator name for debugging. */
    private final String operatorName;

    /**
     * The type info of this wrapped operator's all inputs.
     *
     * <p>NOTE:The inputs of an operator may not all be in the multiple-input operator, e.g. The
     * multiple-input operator contains A and J, and A is one of the input of J, and another input
     * of J is not in the multiple-input operator.
     *
     * <pre>
     * -------
     *        \
     *         J --
     *        /
     * -- A --
     * </pre>
     *
     * <p>For this example, `allInputTypes` contains two input types.
     */
    private final List<TypeInformation<?>> allInputTypes;

    /** The type info of this wrapped operator's output. */
    private final TypeInformation<?> outputType;

    /** Managed memory fraction in the multiple-input operator. */
    private double managedMemoryFraction = -1;

    /** The input edges of this operator wrapper, the edges' target is current instance. */
    private final List<Edge> inputEdges;

    /** The output edges of this operator wrapper, the edges' source is current instance. */
    private final List<Edge> outputEdges;

    /** The wrapped operator, which will be generated by {@link #factory}. */
    private transient OP wrapped;

    private boolean closed;
    private int endedInputCount;

    public TableOperatorWrapper(
            StreamOperatorFactory<RowData> factory,
            String operatorName,
            List<TypeInformation<?>> allInputTypes,
            TypeInformation<?> outputType) {
        this.factory = checkNotNull(factory);
        this.operatorName = checkNotNull(operatorName);
        this.outputType = checkNotNull(outputType);
        this.allInputTypes = checkNotNull(allInputTypes);

        this.inputEdges = new ArrayList<>();
        this.outputEdges = new ArrayList<>();

        this.endedInputCount = 0;
    }

    public void createOperator(StreamOperatorParameters<RowData> parameters) {
        checkArgument(wrapped == null, "This operator has been initialized");
        if (factory instanceof ProcessingTimeServiceAware) {
            ((ProcessingTimeServiceAware) factory)
                    .setProcessingTimeService(parameters.getProcessingTimeService());
        }
        wrapped = factory.createStreamOperator(parameters);
    }

    public void endOperatorInput(int inputId) throws Exception {
        endedInputCount++;
        if (wrapped instanceof BoundedOneInput) {
            ((BoundedOneInput) wrapped).endInput();
            propagateEndOperatorInput();
        } else if (wrapped instanceof BoundedMultiInput) {
            ((BoundedMultiInput) wrapped).endInput(inputId);
            if (endedInputCount >= allInputTypes.size()) {
                propagateEndOperatorInput();
            }
        } else {
            // some batch operators do not extend from BoundedOneInput, such as BatchCalc
            propagateEndOperatorInput();
        }
    }

    private void propagateEndOperatorInput() throws Exception {
        for (Edge edge : outputEdges) {
            edge.target.endOperatorInput(edge.inputId);
        }
    }

    public OP getStreamOperator() {
        return checkNotNull(wrapped);
    }

    public List<TypeInformation<?>> getAllInputTypes() {
        return allInputTypes;
    }

    public TypeInformation<?> getOutputType() {
        return outputType;
    }

    public void addInput(TableOperatorWrapper<?> input, int inputId) {
        Preconditions.checkArgument(inputId > 0 && inputId <= getAllInputTypes().size());
        Edge edge = new Edge(input, this, inputId);
        this.inputEdges.add(edge);
        input.outputEdges.add(edge);
    }

    public void setManagedMemoryFraction(double managedMemoryFraction) {
        this.managedMemoryFraction = managedMemoryFraction;
    }

    public double getManagedMemoryFraction() {
        return managedMemoryFraction;
    }

    public List<Edge> getInputEdges() {
        return inputEdges;
    }

    public List<TableOperatorWrapper<?>> getInputWrappers() {
        return inputEdges.stream().map(Edge::getSource).collect(Collectors.toList());
    }

    public List<Edge> getOutputEdges() {
        return outputEdges;
    }

    public List<TableOperatorWrapper<?>> getOutputWrappers() {
        return outputEdges.stream().map(Edge::getTarget).collect(Collectors.toList());
    }

    /**
     * Checks if the wrapped operator has been closed.
     *
     * <p>Note that this method must be called in the task thread.
     */
    public boolean isClosed() {
        return closed;
    }

    public void close() throws Exception {
        if (isClosed()) {
            return;
        }
        closed = true;
        wrapped.close();
    }

    public String getOperatorName() {
        return operatorName;
    }

    @VisibleForTesting
    public int getEndedInputCount() {
        return endedInputCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableOperatorWrapper<?> other = (TableOperatorWrapper<?>) o;
        boolean b =
                Double.compare(other.managedMemoryFraction, managedMemoryFraction) == 0
                        && factory.equals(other.factory)
                        && operatorName.equals(other.operatorName)
                        && allInputTypes.equals(other.allInputTypes)
                        && outputType.equals(other.outputType)
                        && inputEdges.size() == other.inputEdges.size()
                        && outputEdges.size() == other.outputEdges.size();
        if (b) {
            return true;
        }
        // compare with string value to avoid dead loop between inputEdge and its target
        for (int i = 0; i < inputEdges.size(); ++i) {
            if (!inputEdges.get(i).toString().equals(other.inputEdges.get(i).toString())) {
                return false;
            }
        }
        // compare with string value to avoid dead loop between outputEdge and its source
        for (int i = 0; i < outputEdges.size(); ++i) {
            if (!outputEdges.get(i).toString().equals(other.outputEdges.get(i).toString())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                factory,
                operatorName,
                allInputTypes,
                outputType,
                managedMemoryFraction,
                inputEdges);
    }

    @Override
    public String toString() {
        return operatorName;
    }

    /** The edge connecting two {@link TableOperatorWrapper}s. */
    public static class Edge implements Serializable {
        private static final long serialVersionUID = 1L;

        private final TableOperatorWrapper<?> source;
        private final TableOperatorWrapper<?> target;

        /**
         * The input id (start from 1) corresponding to the target's inputs. e.g. the target is a
         * join operator, depending on the side of the source, input id may be 1 (left side) or 2
         * (right side).
         */
        private final int inputId;

        public Edge(TableOperatorWrapper<?> source, TableOperatorWrapper<?> target, int inputId) {
            this.source = checkNotNull(source);
            this.target = checkNotNull(target);
            this.inputId = inputId;
        }

        public TableOperatorWrapper<?> getSource() {
            return source;
        }

        public TableOperatorWrapper<?> getTarget() {
            return target;
        }

        public int getInputId() {
            return inputId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Edge edge = (Edge) o;
            return inputId == edge.inputId
                    && source.equals(edge.source)
                    && target.equals(edge.target);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, target, inputId);
        }

        @Override
        public String toString() {
            return "Edge{"
                    + "source="
                    + source
                    + ", target="
                    + target
                    + ", inputId="
                    + inputId
                    + '}';
        }
    }
}
