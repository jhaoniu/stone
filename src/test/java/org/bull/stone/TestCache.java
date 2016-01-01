package org.bull.stone;

import org.bull.stone.cache.Cache;
import org.bull.stone.cache.ConcurrentLRUCache;
import org.junit.Test;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-11 下午1:32
 */
public class TestCache {

    @Test
    public void test() {
        Cache<String, String> cache = new ConcurrentLRUCache<>(10, 5);

        cache.put("xxx1", "xxx1");
        cache.put("xxx2", "xxx2");
        cache.put("xxx3", "xxx3");
        cache.put("xxx4", "xxx4");
        cache.put("xxx5", "xxx5");
        cache.put("xxxx1", "xxxx1");
        System.out.println(cache.get("xxx1"));
        cache.put("xxxx2", "xxxx2");
        cache.put("xxxx3", "xxxx3");
        System.out.println(cache.get("xxx1"));
        cache.put("xxxx4", "xxxx4");
        cache.put("xxxx5", "xxxx5");
        cache.put("xxxxx1", "xxxxx1");
        cache.put("xxxxx2", "xxxxx2");
        cache.put("xxxxx3", "xxxxx3");
        cache.put("xxxxx4", "xxxxx4");
        cache.put("xxxxx5", "xxxxx5");
        cache.put("xxxxxx1", "xxxxxx1");
        cache.put("xxxxxx2", "xxxxxx2");
        cache.put("xxxxxx3", "xxxxxx3");
        cache.put("xxxxxx4", "xxxxxx4");
        cache.put("xxxxxx5", "xxxxxx5");

        System.out.println(cache.get("xxx1"));
    }
}
