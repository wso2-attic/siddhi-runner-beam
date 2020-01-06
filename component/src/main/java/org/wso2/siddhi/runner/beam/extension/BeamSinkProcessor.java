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

import org.apache.beam.sdk.util.WindowedValue;
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
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Following extension extracts value from {@link WindowedValue} to send to a Siddhi sink.
 */
@Extension(
        name = "pardo",
        namespace = "beam",
        description = "This stream processor extension extracts values from .\n" +
                " WindowedValue objects to pass into Siddhi sink stream.",
        parameters = {
                @Parameter(name = "event",
                        description = "All WindowedValue<String> objects that will be sent to file sink",
                        type = {DataType.OBJECT})
        },
        examples = @Example(
                syntax = "define stream inputStream (event object);\n" +
                        "@sink(type='file', file.uri='/destination', append='true', " +
                        "@map(type='text', @payload('{{value}}') ))\n" +
                        "define stream outputStream\n" +
                        "@info(name = 'query1')\n" +
                        "from inputStream#beam:sink(event)\n" +
                        "select value, \n" +
                        "insert into outputStream;",
                description = "This query will extract String value from event and sent to file sink stream")
)

public class BeamSinkProcessor extends StreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BeamSinkProcessor.class);
    private ExpressionExecutor eventExecutor;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                WindowedValue element = (WindowedValue) this.eventExecutor.execute(event);
                String newValue = (String) element.getValue();
                Object[] outputObject = {newValue};
                StreamEvent streamEvent = new StreamEvent(0, 0, 1);
                streamEvent.setOutputData(outputObject);
                complexEventChunk.add(streamEvent);
            }
        }
        nextProcessor.process(complexEventChunk);
    }


    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
        List<Attribute> attributes = new LinkedList<>();
        if (attributeExpressionLength != 1) {
            throw new SiddhiAppCreationException("Only 1 parameters can be specified for BeamSinkProcessor");
        } else {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.OBJECT
                && attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
                attributes.add(new Attribute("value", Attribute.Type.STRING));
                this.eventExecutor = attributeExpressionExecutors[0];
            }
        }

        return attributes;
    }


    @Override
    public void start() { }

    @Override
    public void stop() { }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) { }
}
