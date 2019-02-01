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

import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.wso2.siddhi.core.event.ComplexEventChunk;

/**
 * DoFnOperator for partition transform.
 * @param <InputT>
 * @param <OutputT>
 */
public class SiddhiPartitionDoFnOperator<InputT, OutputT> extends SiddhiDoFnOperator {

    private TupleTag secondaryTag;

    public SiddhiPartitionDoFnOperator
            (AppliedPTransform transform, PCollection collection, TupleTag tag) throws Exception {
        super(transform, collection);
        this.secondaryTag = tag;
    }

    @Override
    public void start() {
        this.outputManager = new EventChunkOutputManager(new ComplexEventChunk<>(false),
                this.mainOutputTag, this.secondaryTag);
        this.delegate = new SimpleDoFnRunner<InputT, OutputT>(options, fn, sideInputReader, outputManager,
                mainOutputTag, additionalOutputTags, stepContext, inputCoder, outputCoders, windowingStrategy);
        this.delegate.startBundle();
    }

}
