package org.bull.stone.util;

import org.bull.stone.common.ConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-10 上午10:40
 */

public class ConfigUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);
    private static Properties optDataProperties;

    static {
        optDataProperties = new Properties();
        try {
            InputStream is = ConfigUtils.class.getClassLoader()
                    .getResourceAsStream(
                            ConfigConstants.CONFIG_FILE_NAME);
            optDataProperties.load(is);
        } catch (Exception e) {
            LOGGER.error("load config got exception", e);
        }
    }

    public static String getProperties(String key) {
        return optDataProperties.getProperty(key);
    }

    public static String getProperties(String key, String defaultValue) {
        return optDataProperties.getProperty(key) == null ? defaultValue : optDataProperties.getProperty(key);
    }

    public static Integer getInterger(String key, int defaultValue) {
        return optDataProperties.getProperty(key) == null ? defaultValue : Integer.valueOf(optDataProperties.getProperty(key));
    }
}

