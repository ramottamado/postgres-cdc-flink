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

package dev.ramottamado.java.flink.util.kafka;

import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_AUTO_OFFSET_RESET;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_BOOTSTRAP_SERVER;
import static dev.ramottamado.java.flink.config.ParameterConfig.KAFKA_CONSUMER_GROUP_ID;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;

import dev.ramottamado.java.flink.annotation.PublicEvolving;

/**
 * The {@link KafkaProperties} allows for constructing new {@link Properties} for Kafka consumer from
 * {@link ParameterTool}.
 *
 * @author Tamado Sitohang
 * @since  1.0
 */
@PublicEvolving
public class KafkaProperties {
    /**
     * Get new {@link Properties} from passed {@link ParameterTool}.
     *
     * @param  params           the parameters inside {@link ParameterTool}
     * @return                  the properties for Kafka consumer to use
     * @throws RuntimeException if {@link dev.ramottamado.java.flink.config.ParameterConfig#KAFKA_BOOTSTRAP_SERVER} is
     *                          not set
     * @author                  Tamado Sitohang
     * @since                   1.0
     */
    public static Properties getProperties(ParameterTool params) throws RuntimeException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", params.getRequired(KAFKA_BOOTSTRAP_SERVER));
        properties.setProperty("group.id", params.get(KAFKA_CONSUMER_GROUP_ID, "flink-cdc-consumer"));
        properties.setProperty("auto.offset.reset", params.get(KAFKA_AUTO_OFFSET_RESET, "latest"));

        return properties;
    }
}
