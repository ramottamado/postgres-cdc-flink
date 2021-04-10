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

package dev.ramottamado.java.flink.util;

import static dev.ramottamado.java.flink.config.ParameterConfig.PROPERTIES_FILE;

import java.io.IOException;

import org.apache.flink.api.java.utils.ParameterTool;

import dev.ramottamado.java.flink.annotation.PublicEvolving;
import dev.ramottamado.java.flink.config.ParameterConfig;

/**
 * The {@link ParameterUtils} provides methods to parse arguments into parameters for the application to run.
 *
 * @author Tamado Sitohang
 * @since  1.0
 */
@PublicEvolving
public final class ParameterUtils {
    /**
     * Parse arguments and assign into {@link ParameterTool} parameters.
     *
     * @param  args             the command line arguments
     * @return                  the {@link ParameterTool} parameters
     * @throws RuntimeException if file described in parameter {@link ParameterConfig#PROPERTIES_FILE} is not exist or
     *                          unreadable
     * @author                  Tamado Sitohang
     * @since                   1.0
     */
    public static ParameterTool parseArgs(String[] args) throws RuntimeException {
        ParameterTool params = ParameterTool.fromArgs(args);

        if (params.has(PROPERTIES_FILE)) {
            try {
                params = ParameterTool.fromPropertiesFile(params.getRequired(PROPERTIES_FILE)).mergeWith(params);
            } catch (IOException e) {
                throw new RuntimeException("Cannot read properties file", e);
            }
        }

        return params;
    }
}
