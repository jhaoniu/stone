package org.bull.stone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-6 下午9:23
 */
public class SimpleKeyValueGenerator implements KeyValueGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKeyValueGenerator.class);

    @Override
    public Cell generateKeyValue(String line) {
        try {
            String[] values = line.split("\\t");
            String key = values[0].trim();
            String uuid = values[1];
            return new Cell(key, uuid.getBytes(), System.currentTimeMillis());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }
}
