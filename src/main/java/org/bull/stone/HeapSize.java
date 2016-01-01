package org.bull.stone;


/**
 * Implementations can be asked for an estimate of their size in bytes.
 *
 * @author jhaoniu
 * @description
 * @date 15-12-4 下午4:41
 */
public interface HeapSize {
    /**
     * @return
     */
    public long heapSize();
}
