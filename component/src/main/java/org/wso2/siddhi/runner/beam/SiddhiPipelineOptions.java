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

package org.wso2.siddhi.runner.beam;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Custom configurations for Beam Pipelines.
 */
public interface SiddhiPipelineOptions extends PipelineOptions {

    @Description("Set input target")
    @Default.String("/home/tuan/WSO2/inputs/input-small.csv")
    String getInputFile();
    void setInputFile(String value);

    @Description("Set output target")
    @Default.String("/home/tuan/WSO2/outputs/result")
    String getOutput();
    void setOutput(String value);

}
