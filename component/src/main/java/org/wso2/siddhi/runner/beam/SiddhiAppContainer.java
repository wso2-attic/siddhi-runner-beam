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

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;
import org.wso2.siddhi.runner.beam.extension.BeamGroupByKeyProcessor;
import org.wso2.siddhi.runner.beam.extension.BeamParDoProcessor;
import org.wso2.siddhi.runner.beam.extension.BeamSinkProcessor;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * This is where Siddhi query is generated depending on {@link DirectGraph} which in turn is created
 * depending on structure of {@link org.apache.beam.sdk.Pipeline}.
 */
public class SiddhiAppContainer {

    private static final Logger log = LoggerFactory.getLogger(SiddhiAppContainer.class);
    private SiddhiAppRuntime runtime;
//    private CommittedBundle bundle;
    private List<String> streamDefinitions = new LinkedList<>();
    private List<String> queryDefinitions = new LinkedList<>();
    private Map<String, AppliedPTransform> transformsMap = new HashMap<>();
    private Map<String, PCollection> collectionsMap = new HashMap<>();
    private Map<String, TupleTag> additionalOutputTags = new HashMap<>();
    private DirectGraph graph;
    private int multiSinkcount = 1;

    public SiddhiAppContainer() { }

    public void createSiddhiQuery() {
        log.info("Creating Siddhi Query");
        ExecutionContext context = ExecutionContext.getInstance();
        this.graph = context.getGraph();
        for (CommittedBundle rootBundle: context.getRootBundles()) {
            for (AppliedPTransform transform: graph.getPerElementConsumers(rootBundle.getPCollection())) {
                if (SiddhiAppContainer.compatibleTransform(transform)) {
                    generateSiddhiQueryForTransform(transform, rootBundle.getPCollection());
                } else {
                    log.error("Siddhi does not support " + transform.getTransform().toString() + " at the moment");
                }
            }
        }
    }

    public void createSiddhiRuntime() throws SiddhiParserException {
        try {
            log.info("Creating Siddhi Runtime");
            SiddhiManager siddhiManager = new SiddhiManager();
            StringBuilder siddhiApp = new StringBuilder();
            this.streamDefinitions.forEach(siddhiApp::append);
            this.queryDefinitions.forEach(siddhiApp::append);
            log.debug(siddhiApp.toString());
            siddhiManager.setExtension("beam:pardo", BeamParDoProcessor.class);
            siddhiManager.setExtension("beam:groupbykey", BeamGroupByKeyProcessor.class);
            siddhiManager.setExtension("beam:sink", BeamSinkProcessor.class);
            this.runtime = siddhiManager.createSiddhiAppRuntime(siddhiApp.toString());
            this.runtime.start();
        } catch (SiddhiParserException e) {
            log.error("There is an error in your SiddhiQL", e.getMessage(), e);
            throw e;
        }
    }

    private void generateSiddhiQueryForTransform(AppliedPTransform transform, PCollection keyCollection) {
        /*
        If transform is not in HashMap
         */
        if (this.transformsMap.get(SiddhiAppContainer.generateTransformName(transform.getFullName())) == null) {
            /*
            Add stream definition and to HashMap for given transform
             */
            String streamName = SiddhiAppContainer.generateTransformName(transform.getFullName()) + "Stream";
            String stream = "define stream " + streamName + " (event object);";
            this.streamDefinitions.add(stream);
            this.transformsMap.put(SiddhiAppContainer.generateTransformName(transform.getFullName()), transform);
            this.collectionsMap.put(SiddhiAppContainer.generateTransformName(transform.getFullName()), keyCollection);

            /*
            Create queries for each transform mapped by output collections
             */
            if (transform.getOutputs().isEmpty()) {
                if (transform.getTransform() instanceof TextIO.Write) {
                    String sinkType = "text";
                    String sinkStreamName = "textSinkStream"
                            + (multiSinkcount == 1 ? "" : String.valueOf(multiSinkcount));
                    try {
                        TextIO.TypedWrite textio = ((TextIO.Write) transform.getTransform()).withOutputFilenames();
                        Class<?> cls = textio.getClass();
                        Method getFileNamePrefix = cls.getDeclaredMethod("getFilenamePrefix");
                        getFileNamePrefix.setAccessible(true);
                        ValueProvider.StaticValueProvider provider =
                                (ValueProvider.StaticValueProvider) getFileNamePrefix.invoke(textio);
                        String filePath = provider.get().toString();
                        this.queryDefinitions.add(generateSinkQuery(sinkType, streamName, sinkStreamName));
                        this.streamDefinitions.add(generateSinkStream(sinkType, sinkStreamName, filePath));
                    } catch (NoSuchMethodException | SecurityException exception) {
                        log.error(exception.getMessage(), exception);
                    } catch (Exception exception) {
                        log.error(exception.getMessage(), exception);
                    }
                }
            } else {
                transform.getOutputs().forEach((tag, collection) -> {
                    PCollection outputCollection = (PCollection) collection;
                    TupleTag outputTag = (TupleTag) tag;
                    for (AppliedPTransform nextTransform: this.graph.getPerElementConsumers(outputCollection)) {
                        if (SiddhiAppContainer.compatibleTransform(nextTransform)) {
                            String outputStreamName = SiddhiAppContainer
                                    .generateTransformName(nextTransform.getFullName()) + "Stream";
                            StringBuilder query = new StringBuilder();
                            if (transform.getTransform() instanceof ParDo.MultiOutput) {
                                query.append("from ")
                                        .append(streamName)
                                        .append("#beam:pardo(event, \"")
                                        .append(SiddhiAppContainer.generateTransformName(transform.getFullName()))
                                        .append("\"");
                                if (transform.getOutputs().size() > 1) {
                                    additionalOutputTags.put(outputCollection.getName(), outputTag);
                                    query.append(", \"")
                                            .append(outputCollection.getName())
                                            .append("\") ");
                                } else {
                                    query.append(") ");
                                }
                                query.append("select event insert into ")
                                        .append(outputStreamName)
                                        .append(";");
                            } else if (transform.getTransform() instanceof GroupByKey) {
                                query.append("from ")
                                        .append(streamName)
                                        .append("#beam:groupbykey(event) select event insert into ")
                                        .append(outputStreamName)
                                        .append(";");
                            } else if (transform.getTransform() instanceof Window.Assign) {
                                Window.Assign windowTransform = (Window.Assign) transform.getTransform();
                                if (windowTransform.getWindowFn() instanceof FixedWindows) {
                                    FixedWindows fixedWindow = (FixedWindows) windowTransform.getWindowFn();
                                    Duration size = fixedWindow.getSize();
                                    Duration offSet = fixedWindow.getOffset();
                                    query.append("from ")
                                            .append(streamName)
                                            .append("#window.timeBatch(")
                                            .append(size.getStandardSeconds())
                                            .append(" sec");
                                    if (offSet == Duration.ZERO) {
                                        query.append(")");
                                    } else {
                                        query.append(", ")
                                                .append(offSet.getStandardSeconds())
                                                .append(" sec)");
                                    }
                                    query.append(" select event insert into ")
                                            .append(outputStreamName)
                                            .append(";");
                                } else if (windowTransform.getWindowFn() instanceof GlobalWindows) {
                                    query.append("from ")
                                            .append(streamName)
                                            .append(" select event insert into ")
                                            .append(outputStreamName)
                                            .append(";");
                                }
                            } else {
                                query.append("from ")
                                        .append(streamName)
                                        .append(" select event insert into ")
                                        .append(outputStreamName)
                                        .append(";");
                            }
                            this.queryDefinitions.add(query.toString());
                            generateSiddhiQueryForTransform(nextTransform, outputCollection);
                        } else {
                            log.error("Siddhi does not support " +
                                    nextTransform.getTransform().toString() + " at the moment");
                            return;
                        }
                    }
                });
            }
        }
    }

    private static boolean compatibleTransform(AppliedPTransform transform) {
        if (transform.getTransform() instanceof ParDo.MultiOutput) {
            return true;
        }
        if (transform.getTransform() instanceof GroupByKey) {
            return true;
        }
        if (transform.getTransform() instanceof Window.Assign) {
            if (((Window.Assign) transform.getTransform()).getWindowFn() instanceof FixedWindows) {
                return true;
            }
            if (((Window.Assign) transform.getTransform()).getWindowFn() instanceof GlobalWindows) {
                return true;
            }
        }
        if (transform.getTransform() instanceof TextIO.Write) {
            return true;
        }
        return transform.getTransform() instanceof Flatten.PCollections;
    }

    private String generateSinkQuery(String sinkType, String streamName, String sinkStreamName) {
        StringBuilder query = new StringBuilder();
        if (sinkType.equals("text")) {
            query.append("from ")
                    .append(streamName)
                    .append("#beam:sink(event) select value insert into ")
                    .append(sinkStreamName)
                    .append(";");
        }
        return query.toString();
    }

    private String generateSinkStream(String sinkType, String sinkStreamName, String filePath) {
        StringBuilder sinkStream = new StringBuilder();
        if (sinkType.equals("text")) {
            sinkStream.append("@sink(type='file', file.uri='")
                    .append(filePath)
                    .append("', append='true', @map(type='text', @payload('{{value}}') ))");
            sinkStream.append(" define stream ")
                    .append(sinkStreamName)
                    .append(" (value string);");
        }
        this.multiSinkcount += 1;
        return sinkStream.toString();
    }

    SiddhiAppRuntime getSiddhiRuntime() {
        return this.runtime;
    }

    Map<String, AppliedPTransform> getTransformsMap() {
        return this.transformsMap;
    }

    Map<String, PCollection> getCollectionsMap() {
        return this.collectionsMap;
    }

    Map<String, TupleTag> getAdditionalOutputTags() {
        return this.additionalOutputTags;
    }

    static String generateTransformName(String value) {
        return value.replace('/', '_').replace('(', '_').replace(")", "").replace('.', '_');
    }

}
