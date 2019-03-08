/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * Manage pipeline execution.
 */
public class SiddhiExecutorService {

    private static final Logger log = LoggerFactory.getLogger(SiddhiExecutorService.class);
    private final int targetParallelism;

    SiddhiExecutorService(int targetParallelism) {
        this.targetParallelism = targetParallelism;
    }

    void start(DirectGraph graph) {
        log.debug("Starting Siddhi Runner");
        ExecutionContext context = ExecutionContext.getInstance();
        context.setGraph(graph);
        try {
            // Generate sources from root transforms
            for (AppliedPTransform rootTransform: graph.getRootTransforms()) {
                ReadEvaluator evaluator = new ReadEvaluator(rootTransform);
                evaluator.execute(this.targetParallelism);
            }
            // Create SiddhiAppRuntime
            SiddhiAppContainer executionRuntime = new SiddhiAppContainer();
            executionRuntime.createSiddhiQuery();
            context.setTransformsMap(executionRuntime.getTransformsMap());
            context.setCollectionsMap(executionRuntime.getCollectionsMap());
            context.setAdditionalOutputTags(executionRuntime.getAdditionalOutputTags());
            executionRuntime.createSiddhiRuntime();
            // Emit elements to SiddhiAppContainer
            log.debug("Executing pipeline");
            for (CommittedBundle<SourceWrapper> rootBundle: context.getRootBundles()) {
                SourceWrapper source = rootBundle.getSourceWrapper();
                source.open();
                for (AppliedPTransform transform: graph.getPerElementConsumers(rootBundle.getPCollection())) {
                    String inputStream = SiddhiAppContainer.generateTransformName(transform.getFullName()) + "Stream";
                    source.run(executionRuntime.getSiddhiRuntime().getInputHandler(inputStream));
                }
            }
            context.clearAllBundles();
        } catch (IOException exception) {
            log.error("Invalid source file destination ", exception.getMessage(), exception);
        } catch (InterruptedException exception) {
            log.error("Unable to send events ", exception.getMessage(), exception);
        } catch (Exception exception) {
            log.error("Unable to execute the service ", exception.getMessage(), exception);
        }
    }

}
