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

package org.wso2.siddhi.runner.beam.extension;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.runner.beam.ExecutionContext;
import org.wso2.siddhi.runner.beam.SiddhiDoFnOperator;
import org.wso2.siddhi.runner.beam.SiddhiPartitionDoFnOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Following extension executes Beam ParDo transform in Siddhi context.
 */
@Extension(
        name = "pardo",
        namespace = "beam",
        description = "This stream processor extension performs ParDo transformation.\n" +
                " for WindowedValue objects when executing a Beam pipeline.",
        parameters = {
                @Parameter(name = "event",
                        description = "All the events of type WindowedValue arriving" +
                                " in chunk to execute ParDo transform",
                        type = {DataType.OBJECT})
        },
        examples = @Example(
                syntax = "define stream inputStream (event object);\n" +
                        "@info(name = 'query1')\n" +
                        "from inputStream#beam:pardo(event)\n" +
                        "select event\n" +
                        "insert into outputStream;",
                description = "This query performs Beam ParDo transformation to all events arriving to inputStream")
)

public class BeamParDoProcessor extends StreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(BeamParDoProcessor.class);
    private SiddhiDoFnOperator operator;
    private ExpressionExecutor eventExecutor;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        ComplexEventChunk<StreamEvent> complexEventChunk;
        synchronized (this) {
            operator.start();
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                WindowedValue value = (WindowedValue) this.eventExecutor.execute(event);
                this.operator.processElement(value);
            }
            this.operator.finish();
            complexEventChunk = this.operator.getOutputChunk();
        }
        nextProcessor.process(complexEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        boolean isTagAvailable;

        if (attributeExpressionLength != 3) {
            if (attributeExpressionLength != 2) {
                throw new SiddhiAppCreationException(
                        "Invalid parameter count. Minimum of 2 values are required."
                );
            } else {
                isTagAvailable = false;
            }
        } else {
            isTagAvailable = true;
        }

        if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.OBJECT
                && attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            this.eventExecutor = attributeExpressionExecutors[0];
        } else {
            throw new SiddhiAppCreationException("First parameter must be a variable object type");
        }

        //Get beam transform here and create DoFnOperator
        try {
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING
                    && attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                String beamTransform = ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                        .getValue().toString();
                ExecutionContext context = ExecutionContext.getInstance();
                AppliedPTransform transform = context.getTransfromFromName(beamTransform);
                PCollection collection = context.getCollectionFromName(beamTransform);
                if (isTagAvailable) {
                    String tagKey = attributeExpressionExecutors[2].execute(null).toString();
                    TupleTag tag = context.getTupleTagFromName(tagKey);
                    this.operator = new SiddhiPartitionDoFnOperator(transform, collection, tag);
                } else {
                    this.operator = new SiddhiDoFnOperator(transform, collection);
                }
                this.operator.start();
            } else {
                throw new SiddhiAppCreationException("Second parameter must be a constant String");
            }
        } catch (Exception e) {
            log.error("Could not execute pardo transform", e.getMessage(), e);
            throw new SiddhiAppCreationException(e);
        }

        return attributes;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {

        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }
}
