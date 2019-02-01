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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Keep a mapping of siddhi stream name and it's relevant
 * {@link org.apache.beam.sdk.transforms.PTransform} and {@link PCollection} and
 * root bundles of the provided {@link org.apache.beam.sdk.Pipeline}.
 */
public class ExecutionContext {

    private static final ExecutionContext context = new ExecutionContext();
    private DirectGraph graph;
    private final Map<PCollection, CommittedBundle> bundles = new HashMap<>();
    private final Map<PCollection, CommittedBundle> rootBundles = new HashMap<>();
    private Map<String, AppliedPTransform> transformsMap = new HashMap<>();
    private Map<String, PCollection> collectionsMap = new HashMap<>();
    private Map<String, TupleTag> additionalOutputTags = new HashMap<>();

    private ExecutionContext() {}

    void setGraph(DirectGraph graph) {
        this.graph = graph;
    }

    public static ExecutionContext getInstance() {
        return context;
    }

    public void addRootBundle(CommittedBundle bundle) {
        this.bundles.put(bundle.getPCollection(), bundle);
        this.rootBundles.put(bundle.getPCollection(), bundle);
    }

    Collection<CommittedBundle> getRootBundles() {
        return rootBundles.values();
    }

    public void clearAllBundles() {
        this.bundles.clear();
        this.rootBundles.clear();
    }

    DirectGraph getGraph() {
        return this.graph;
    }

    void setTransformsMap(Map<String, AppliedPTransform> map) {
        this.transformsMap = map;
    }

    void setCollectionsMap(Map<String, PCollection> map) {
        this.collectionsMap = map;
    }

    void setAdditionalOutputTags(Map<String, TupleTag> map) {
        this.additionalOutputTags = map;
    }

    public AppliedPTransform getTransfromFromName(String key) {
        return this.transformsMap.get(key);
    }

    public PCollection getCollectionFromName(String key) {
        return this.collectionsMap.get(key);
    }

    public TupleTag getTupleTagFromName(String key) {
        return this.additionalOutputTags.get(key);
    }

}
