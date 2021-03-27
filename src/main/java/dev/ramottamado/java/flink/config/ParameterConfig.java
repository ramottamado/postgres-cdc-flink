/*
 * Copyright 2021 Tamado Sitohang <ramot@ramottamado.dev>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.ramottamado.java.flink.config;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * The main class to place constants for {@link ParameterTool}.
 * The constants will be the parameters when running the application.
 */
public final class ParameterConfig {
    public static final String CHECKPOINT_PATH = "checkpoint-path";
    public static final String DEBUG_RESULT_STREAM = "debug-result-stream";
    public static final String ENVIRONMENT = "environment";
    public static final String KAFKA_AUTO_OFFSET_RESET = "auto-offset-reset";
    public static final String KAFKA_BOOTSTRAP_SERVER = "bootstrap-server";
    public static final String KAFKA_CONSUMER_GROUP_ID = "consumer-group-id";
    public static final String KAFKA_OFFSET_STRATEGY = "offset-strategy";
    public static final String KAFKA_SOURCE_TOPIC_1 = "source-topic-1";
    public static final String KAFKA_SOURCE_TOPIC_2 = "source-topic-2";
    public static final String KAFKA_TARGET_TOPIC = "target-topic";
    public static final String PROPERTIES_FILE = "properties-file";

    private ParameterConfig() {
    }
}
