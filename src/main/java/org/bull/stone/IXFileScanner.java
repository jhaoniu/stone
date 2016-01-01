package org.bull.stone;

import java.io.IOException;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-5 上午9:54
 */
public interface IXFileScanner {
    /**
     * Look at the next Cell in this scanner, but do not iterate scanner.
     *
     * @return the next Cell
     */
    public Cell peek() throws IOException;

    /**
     * Return the next Cell in this scanner, iterating the scanner
     *
     * @return the next Cell
     */
    public Cell next() throws IOException;

    /**
     * close handle
     *
     * @throws IOException
     */
    public void close() throws IOException;

}
