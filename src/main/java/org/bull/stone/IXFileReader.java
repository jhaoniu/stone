package org.bull.stone;

import com.google.common.util.concurrent.FutureCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-4 下午9:58
 */
public interface IXFileReader {
    /**
     * @param key
     * @param timeout
     * @param futureCallback
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    public Cell get(String key, long timeout, FutureCallback<Cell> futureCallback) throws IOException, TimeoutException;

    /**
     * use when need to load a new index
     *
     * @throws IOException
     */
    public void reOpenReader() throws IOException;
}
