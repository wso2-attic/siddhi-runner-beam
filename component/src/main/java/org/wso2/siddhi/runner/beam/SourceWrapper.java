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
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Construct a wrapper for {@link BoundedSource}.
 * @param <OutputT>
 */
public class SourceWrapper<OutputT> {

    private static final Logger log = LoggerFactory.getLogger(SourceWrapper.class);
    private List<? extends BoundedSource<OutputT>> splitSources;
    private PipelineOptions options;
    private List<BoundedSource<OutputT>> localSplitSources;
    private List<BoundedReader<OutputT>> localReaders;
    private List<Event> elements = new LinkedList<>();

    public SourceWrapper(BoundedSource<OutputT> source, int parallelism, PipelineOptions options) throws Exception {
        this.options = options;
        long estimatedBytes = source.getEstimatedSizeBytes(options);
        long bytesPerBundle = estimatedBytes / (long) parallelism;
        this.splitSources = source.split(bytesPerBundle, options);
        this.localSplitSources = new LinkedList<>();
        this.localReaders = new LinkedList<>();
    }

    public void open() throws IOException {
        for (BoundedSource<OutputT> source: this.splitSources) {
            BoundedReader<OutputT> reader = source.createReader(this.options);
            this.localSplitSources.add(source);
            this.localReaders.add(reader);
        }
    }

    public void run(InputHandler inputHandler) throws InterruptedException {

        /*
         Run the source to emit each element to DoFnOperator delegate
         */
        try {
            for (BoundedReader<OutputT> reader: this.localReaders) {
                boolean hasData = reader.start();
                while (hasData) {
                    WindowedValue elem = WindowedValue
                            .timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp());
                    this.convertToEvent(elem);
                    hasData = reader.advance();
                }
            }
            Event[] stream = elements.toArray(new Event[0]);
            inputHandler.send(stream);
        } catch (IOException exception) {
            log.error("Error while reading source file ", exception.getMessage(), exception);
        }

    }

    private void convertToEvent(WindowedValue elem) {
        Event event = new Event();
        event.setData(new Object[]{elem});
        elements.add(event);
    }

}
