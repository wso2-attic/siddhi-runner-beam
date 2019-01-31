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

import com.google.common.collect.Multimap;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import java.util.Collection;
import java.util.Set;

/**
 * Methods for interacting with the underlying structure of a {@link org.apache.beam.sdk.Pipeline} that is being
 * executed with the {@link SiddhiRunner}.
 */
public class DirectGraph {

    private final Multimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers;
    private final Set<AppliedPTransform<?, ?, ?>> rootTransforms;

    public static DirectGraph create(Multimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
                                     Set<AppliedPTransform<?, ?, ?>> rootTransforms) {
        return new DirectGraph(perElementConsumers, rootTransforms);
    }

    private DirectGraph(Multimap<PInput, AppliedPTransform<?, ?, ?>> perElementConsumers,
                        Set<AppliedPTransform<?, ?, ?>> rootTransforms) {
        this.perElementConsumers = perElementConsumers;
        this.rootTransforms = rootTransforms;
    }

    public Set<AppliedPTransform<?, ?, ?>> getRootTransforms() {
        return this.rootTransforms;
    }

    public Collection<AppliedPTransform<?, ?, ?>> getPerElementConsumers(PValue consumed) {
        return this.perElementConsumers.get(consumed);
    }

}
