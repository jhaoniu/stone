package org.bull.stone;

import java.io.IOException;

/**
 * @author jhaoniu
 * @description
 * @date 15-10-23 下午5:48
 */
public interface IMemTable {
    /**
     * @param cell
     */
    public void append(Cell cell) throws IOException;

    /**
     * @param key
     * @return
     */
    public Cell get(String key);


    public void flush() throws IOException;
}
