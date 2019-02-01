# siddhi-runner-beam

The siddhi-runner-beam is a feature that allows you to execute a Beam pipeline via Siddhi. When the Beam program is set to Siddhi runner, a siddhi application is created and deployed and the pipeline is executed.

Siddhi runner currently performs the following Beam transformations:

+ ParDo
+ GroupByKey
+ Fixed Windowing
+ TextIO
+ Flatten
+ Partition

In order to execute a Beam pipeline, you can create a Maven project and then follow the steps below:

## Step 1: Define a Beam pipeline

For detailed instructions to define a Beam pipeline, see [Apache Beam Documentation - Create Your Pipeline.](https://beam.apache.org/documentation/pipelines/create-your-pipeline/)

## Step 2: Set the runner

In your Maven project, add the following class to set the runner as `SiddhiRunner`.

```java
//SiddhiPipelineOptions is defined
SiddhiPipelineOptions options = PipelineOptionsFactory.as(SiddhiPipelineOptions.class);
options.setRunner(SiddhiRunner.class);
```

