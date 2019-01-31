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

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;

/**
 * This produces {@link Read.Bounded} from primitive {@link PTransform} and outputs to a {@link CommittedBundle}.
 * @param <T> Type of records read by the source
 */
public class ReadEvaluator<T> {

    private AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform;

    ReadEvaluator(AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform) {
        this.transform = transform;
    }

    void execute(int parallels) throws Exception {
        Read.Bounded<T> boundedInput = (Read.Bounded<T>) this.transform.getTransform();
        BoundedSource<T> source = boundedInput.getSource();
        SourceWrapper sourceWrapper = new SourceWrapper(source, parallels, transform.getPipeline().getOptions());
        ExecutionContext context = ExecutionContext.getInstance();
        for (PValue collection: this.transform.getOutputs().values()) {
            CommittedBundle<SourceWrapper> bundle = new CommittedBundle<>((PCollection) collection);
            bundle.addItem(sourceWrapper);
            context.addRootBundle(bundle);
        }
    }

}
