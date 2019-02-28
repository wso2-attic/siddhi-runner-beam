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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * A {@link PipelineRunner} that executes the operations in the pipeline by translating them
 * to a Siddhi Query and deploy in a local or remote {@link org.wso2.siddhi.core.SiddhiAppRuntime}.
 */
public class SiddhiRunner extends PipelineRunner<PipelineResult> {

    public static SiddhiRunner fromOptions(PipelineOptions options) {
        PipelineOptionsValidator.validate(SiddhiPipelineOptions.class, options);
        return new SiddhiRunner();
    }

    private SiddhiRunner() { }

    @Override
    public PipelineResult run(Pipeline pipeline) {
        int targetParallelism = 1;
        GraphVisitor graphVisitor = new GraphVisitor();
        pipeline.traverseTopologically(graphVisitor);
        DirectGraph graph = graphVisitor.getGraph();
        SiddhiExecutorService executor = new SiddhiExecutorService(targetParallelism);
        executor.start(graph);
        return new SiddhiRunnerResult();
    }

    /**
     * Not supported right now.
     */
    public static class SiddhiRunnerResult implements PipelineResult {
        private State state;

        public SiddhiRunnerResult() {
            this.state = State.RUNNING;
        }

        @Override
        public State getState() {
            return this.state;
        }

        @Override
        public State cancel() throws IOException {
            return this.state;
        }

        @Override
        public State waitUntilFinish(Duration duration) {
            return this.state;
        }

        @Override
        public State waitUntilFinish() {
            return this.state;
        }

        @Override
        public MetricResults metrics() {
            return new SiddhiMetrics();
        }
    }

}
