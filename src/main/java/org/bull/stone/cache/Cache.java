package org.bull.stone.cache;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-11 上午10:50
 */
public interface Cache<K, V> {
    public V put(K key, V val);

    public V get(K key);

    public V remove(K key);

    public void clear();

}
