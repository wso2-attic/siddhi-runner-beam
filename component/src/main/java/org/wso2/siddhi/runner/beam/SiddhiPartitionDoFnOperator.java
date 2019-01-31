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
