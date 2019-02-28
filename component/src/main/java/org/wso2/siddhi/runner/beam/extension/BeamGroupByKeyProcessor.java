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
import org.apache.beam.sdk.values.KV;
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
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Following extension executes GroupByKey Beam transform in Siddhi context.
 * @param <K> Type of key in {@link KV}
 * @param <V> Type of value in {@link KV}
 */

@Extension(
        name = "groupbykey",
        namespace = "beam",
        description = "This stream processor extension performs grouping of events by key" +
                " for WindowedValue objects when executing a Beam pipeline.",
        parameters = {
                @Parameter(name = "event",
                        description = "All the events of type WindowedValue arriving in " +
                                "chunk to execute GroupByKey transform",
                        type = {DataType.OBJECT})
        },
        examples = @Example(
                syntax = "define stream inputStream (event object);\n" +
                        "@info(name = 'query1')\n" +
                        "from inputStream#beam:groupbykey(event)\n" +
                        "select event\n" +
                        "insert into outputStream;",
                description = "This query performs Beam GroupByKey transformation provided WindowedValue<KV> as event")
)

public class BeamGroupByKeyProcessor<K, V> extends StreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(BeamGroupByKeyProcessor.class);
    private ExpressionExecutor eventExecutor;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<>(false);
        Map<K, List<V>> groupByKeyMap = new HashMap<>();
        synchronized (this) {
            try {
                while (streamEventChunk.hasNext()) {
                    StreamEvent event = streamEventChunk.next();
                    WindowedValue windowedValue = (WindowedValue) this.eventExecutor.execute(event);
                    KV keyValueElement = (KV) windowedValue.getValue();
                    if (groupByKeyMap.containsKey(keyValueElement.getKey())) {
                        List<V> items = groupByKeyMap.get(keyValueElement.getKey());
                        items.add((V) keyValueElement.getValue());
                        groupByKeyMap.put((K) keyValueElement.getKey(), items);
                    } else {
                        List<V> item = new LinkedList<>();
                        item.add((V) keyValueElement.getValue());
                        groupByKeyMap.put((K) keyValueElement.getKey(), item);
                    }
                }
                for (Map.Entry map: groupByKeyMap.entrySet()) {
                    K key = (K) map.getKey();
                    List<V> value = (LinkedList<V>) map.getValue();
                    KV kv = KV.of(key, value);
                    StreamEvent streamEvent = new StreamEvent(0, 0, 1);
                    streamEvent.setOutputData(WindowedValue.valueInGlobalWindow(kv), 0);
                    complexEventChunk.add(streamEvent);
                }
            } catch (Exception e) {
                log.error("Unable to perform GroupByKey operation ", e.getMessage(), e);
            }
        }
        nextProcessor.process(complexEventChunk);

    }


    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

        if (attributeExpressionLength != 1) {
            throw new SiddhiAppCreationException(
                    "Only one parameter can be specified for BeamGroupByKeyProcessor but " + attributeExpressionLength
                            + " were given"
            );
        }

        if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.OBJECT) {
            this.eventExecutor = attributeExpressionExecutors[0];
        } else {
            throw new SiddhiAppCreationException(
                    "First parameter should be of type Object but "
                    + attributeExpressionExecutors[0].getReturnType().toString()
                    + " was given"
            );
        }
        return new LinkedList<>();
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
