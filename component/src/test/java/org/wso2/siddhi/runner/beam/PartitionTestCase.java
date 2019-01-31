package org.wso2.siddhi.runner.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
import org.testng.TestException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class PartitionTestCase {

    String rootPath, source, sink, sink2;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = PartitionTestCase.class.getClassLoader();
        rootPath = classLoader.getResource("files").getFile();
        source = rootPath + "/inputs/sample.txt";
        sink = rootPath + "/sink/partitionResult.txt";
        sink2 = rootPath + "/sink/partitionResult2.txt";
    }

    @AfterMethod
    public void doAfterMethod() {
        try {
            FileUtils.deleteDirectory(new File(rootPath + "/sink"));
        } catch (IOException e) {
            throw new TestException("Failed to delete files in due to " + e.getMessage(), e);
        }
    }

    private static class ModFn implements PartitionFn<String> {

        @Override
        public int partitionFor(String elem, int numPartitions) {
            return Integer.parseInt(elem) % numPartitions;
        }
    }


    private void runPartition(SiddhiPipelineOptions options) {
        Pipeline pipe = Pipeline.create(options);
        PCollectionList<String> outputs = pipe.apply(Create.of("1", "2", "3", "4", "5"))
                .apply(Partition.of(2, new ModFn()));
        PCollection<String> collection = outputs.get(0);
        PCollection<String> collection2 = outputs.get(1);
        collection.apply(TextIO.write().to(sink));
        collection2.apply(TextIO.write().to(sink2));
        pipe.run();
    }

    @Test
    public void partitionTestCase() throws InterruptedException {
        SiddhiPipelineOptions options = PipelineOptionsFactory.as(SiddhiPipelineOptions.class);
        options.setRunner(SiddhiRunner.class);
        runPartition(options);
        Thread.sleep(500);

        int result1[] = {2, 4};
        File sinkFile = new File(sink);
        try {
            if (sinkFile.isFile()) {
                BufferedReader reader = new BufferedReader(new FileReader(sinkFile));
                String line;
                int i = 0;
                while ((line = reader.readLine()) != null) {
                    AssertJUnit.assertEquals(Integer.parseInt(line), result1[i]);
                    i++;
                }
            } else {
                AssertJUnit.fail(sink + " is not a directory");
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + sinkFile.getAbsolutePath());
        }

        int result2[] = {1, 3, 5};
        sinkFile = new File(sink2);
        try {
            if (sinkFile.isFile()) {
                BufferedReader reader = new BufferedReader(new FileReader(sinkFile));
                String line;
                int i = 0;
                while ((line = reader.readLine()) != null) {
                    AssertJUnit.assertEquals(Integer.parseInt(line), result2[i]);
                    i++;
                }
            } else {
                AssertJUnit.fail(sink + " is not a directory");
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + sinkFile.getAbsolutePath());
        }
    }

}
