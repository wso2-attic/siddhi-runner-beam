package org.wso2.siddhi.runner.beam;

import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;

/**
 * A {@link DoFnRunners.OutputManager} that can buffer its outputs using {@link ComplexEventChunk}.
 */
public class EventChunkOutputManager implements DoFnRunners.OutputManager {

    private ComplexEventChunk<StreamEvent> outputChunk;
    private TupleTag mainOutputTag;
    private TupleTag secondaryOutputTag;

    EventChunkOutputManager(ComplexEventChunk<StreamEvent> complexEventChunk,
                            TupleTag mainOutputTag) {
        this.outputChunk = complexEventChunk;
        this.mainOutputTag = mainOutputTag;
        this.secondaryOutputTag = null;
    }

    EventChunkOutputManager(ComplexEventChunk<StreamEvent> complexEventChunk,
                            TupleTag mainOutputTag, TupleTag secondaryOutputTag) {
        this.outputChunk = complexEventChunk;
        this.mainOutputTag = mainOutputTag;
        this.secondaryOutputTag = secondaryOutputTag;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        if (tag.equals(mainOutputTag) || tag.equals(secondaryOutputTag)) {
            StreamEvent streamEvent = new StreamEvent(0, 0, 1);
            streamEvent.setOutputData(output, 0);
            this.outputChunk.add(streamEvent);
        }
    }

    public ComplexEventChunk getOutputChunk() {
        return this.outputChunk;
    }
}
