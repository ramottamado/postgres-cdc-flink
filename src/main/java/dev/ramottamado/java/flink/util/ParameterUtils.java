package dev.ramottamado.java.flink.util;

import static dev.ramottamado.java.flink.config.ParameterConfig.PROPERTIES_FILE;

import java.io.IOException;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * The {@link ParameterUtils} provides methods to parse arguments into parameters for the application to run.
 */
public final class ParameterUtils {
    /**
     * Parse arguments and assign into {@link ParameterTool} parameters.
     *
     * @param  args             the command line arguments
     * @return                  the {@link ParameterTool} parameters
     * @throws RuntimeException if file described in parameter {@link PROPERTIES_FILE} is not exist or unreadable
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
