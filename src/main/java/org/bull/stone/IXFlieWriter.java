package org.bull.stone;

import java.io.IOException;

/**
 * @author jhaoniu
 * @description
 * @date 15-10-27 下午3:47
 */
public interface IXFlieWriter {
    /**
     * @param cell
     * @throws java.io.IOException
     */
    public void append(Cell cell) throws IOException;

    /**
     * @throws java.io.IOException
     */
    public void close() throws IOException;

}
