package dev.ramottamado.java.flink.util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParameterUtils extends Constants {

    // No reflection
    private ParameterUtils() {
        throw new UnsupportedOperationException("Should not be instantiated");
    }

    private static final Logger logger = LoggerFactory.getLogger(ParameterUtils.class);

    public static ParameterTool parseArgs(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        if (params.has(PROPERTIES_FILE)) {
            logger.info("Getting parameters from properties file");
            params = ParameterTool.fromPropertiesFile(params.getRequired(PROPERTIES_FILE)).mergeWith(params);
        }

        return params;
    }

}
