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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
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
import java.util.Arrays;
import java.util.Iterator;

public class FlattenWithoutWindowTestCase {

    private String rootPath, source, sink;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = MultiParDoTestCase.class.getClassLoader();
        rootPath = classLoader.getResource("files").getFile();
        source = rootPath + "/inputs/sample.csv";
        sink = rootPath + "/sink/flattenResult.txt";
    }

    @AfterMethod
    public void doAfterMethod() {
        try {
            FileUtils.deleteDirectory(new File(rootPath + "/sink"));
        } catch (IOException e) {
            throw new TestException("Failed to delete files in due to " + e.getMessage(), e);
        }
    }

    private static class CheckElement extends DoFn<String, KV<String, String[]>> {

        String[] regions = {"Asia", "Central America"};

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<KV<String, String[]>> out) {
            String[] words = element.split(",");
            if (Arrays.asList(regions).contains(words[0].trim())) {
                KV<String, String[]> kv = KV.of(words[0].trim(), Arrays.copyOfRange(words, 1, words.length));
                out.output(kv);
            }
        }

    }

    public static class FindKeyValueFn extends SimpleFunction<KV<String, Iterable<String[]>>, String> {

        @Override
        public String apply(KV<String, Iterable<String[]>> input) {
            Iterator<String[]> iter = input.getValue().iterator();
            float totalProfit = 0f;
            while (iter.hasNext()) {
                String[] details = iter.next();
                totalProfit += Float.parseFloat(details[details.length - 1]);
            }
            return input.getKey().trim() + ":" + totalProfit;
        }

    }

    private static class CSVFilterRegion extends PTransform<PCollection<String>, PCollection<KV<String, String[]>>> {

        public PCollection<KV<String, String[]>> expand(PCollection<String> lines) {
            return lines.apply(ParDo.of(new CheckElement()));
        }

    }

    @Test
    public void flattenWithoutWindowTest() throws InterruptedException {
        SiddhiPipelineOptions options = PipelineOptionsFactory.as(SiddhiPipelineOptions.class);
        options.setRunner(SiddhiRunner.class);
        runFlattenWithoutWindow(options);
        Thread.sleep(1000);
        File sinkFile = new File(sink);
        try {
            if (sinkFile.isFile()) {
                BufferedReader reader = new BufferedReader(new FileReader(sinkFile));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] continents = line.split(":");
                    switch(continents[0]) {
                        case "Central America":
                            AssertJUnit.assertEquals(150000.0f, Float.valueOf(continents[1]));
                            break;
                        case "Asia":
                            AssertJUnit.assertEquals(480000.0f, Float.valueOf(continents[1]));
                            break;
                        default:
                            AssertJUnit.fail("Invalid value in sink file " + Arrays.toString(continents));
                    }
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

    private void runFlattenWithoutWindow(SiddhiPipelineOptions options) {
        Pipeline pipe = Pipeline.create(options);
        PCollection<KV<String, String[]>> collectionOne = pipe.apply(TextIO.read()
                .from(source))
                .apply(new CSVFilterRegion());
        PCollection<KV<String, String[]>> collectionTwo = pipe.apply(TextIO.read()
                .from(source))
                .apply(new CSVFilterRegion());
        PCollectionList<KV<String, String[]>> collectionList = PCollectionList
                .of(collectionOne).and(collectionTwo);
        PCollection<KV<String, String[]>> merged = collectionList
                .apply(Flatten.pCollections());
        merged.apply(GroupByKey.create()).apply(MapElements.via(new FindKeyValueFn()))
            .apply(TextIO.write().to(sink));
        pipe.run();
    }
}
