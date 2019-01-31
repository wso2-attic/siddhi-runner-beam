/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.runner.beam;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * This is a graph visitor which traverse the {@link Pipeline} to extract the transforms
 * required by {@link SiddhiRunner} to execute user-provided {@link org.apache.beam.sdk.values.PCollection}
 * based job to create {@link DirectGraph}.
 */
public class GraphVisitor extends Pipeline.PipelineVisitor.Defaults {

    private static final Logger log = LoggerFactory.getLogger(GraphVisitor.class);
    private Set<AppliedPTransform<?, ?, ?>> rootTransforms = new HashSet<>();
    private Multimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers = HashMultimap.create();
    private Multimap<PValue, AppliedPTransform<?, ?, ?>> allConsumers = HashMultimap.create();
    private int numTransforms = 0;
    private int depth;

    public CompositeBehavior enterCompositeTransform(Node node) {
        if (node.getTransform() instanceof TextIO.Write) {
            AppliedPTransform<?, ?, ?> appliedPTransform = this.getAppliedTransform(node);
            Collection<PValue> mainInputs = TransformInputs.nonAdditionalInputs(
                    node.toAppliedPTransform(this.getPipeline())
            );
            for (PValue pValue: mainInputs) {
                this.perElementConsumers.put(pValue, appliedPTransform);
            }
            for (PValue pValue: node.getInputs().values()) {
                this.allConsumers.put(pValue, appliedPTransform);
            }
            return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
        }
        log.info("{} enterCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
        ++this.depth;
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    public void leaveCompositeTransform(Node node) {
        --this.depth;
        log.info("{} leaveCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
    }

    public void visitPrimitiveTransform(Node node) {
        log.info("{} visitPrimitiveTransform- {}", genSpaces(this.depth), node.getFullName());

        AppliedPTransform appliedPTransform = this.getAppliedTransform(node);
        if (node.getInputs().isEmpty()) {
            if (appliedPTransform.getTransform() instanceof Read.Bounded) {
                this.rootTransforms.add(appliedPTransform);
            }
        } else {
            Collection<PValue> mainInputs = TransformInputs.nonAdditionalInputs(
                    node.toAppliedPTransform(this.getPipeline())
            );
            Iterator iter = mainInputs.iterator();
            PValue value;
            while (iter.hasNext()) {
                value = (PValue) iter.next();
                this.perElementConsumers.put(value, appliedPTransform);
            }
            iter = node.getInputs().values().iterator();
            while (iter.hasNext()) {
                value = (PValue) iter.next();
                this.allConsumers.put(value, appliedPTransform);
            }
        }
    }

    private AppliedPTransform<?, ?, ?> getAppliedTransform(Node node) {
        return node.toAppliedPTransform(this.getPipeline());
    }

    public DirectGraph getGraph() {
        return DirectGraph.create(perElementConsumers, rootTransforms);
    }

    protected static String genSpaces(int n) {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < n; ++i) {
            builder.append("|   ");
        }

        return builder.toString();
    }


}
