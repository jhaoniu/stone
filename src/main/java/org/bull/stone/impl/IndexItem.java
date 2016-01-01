package org.bull.stone.impl;

/**
* @author jhaoniu
* @description
* @date 15-12-4 下午10:02
*/
class IndexItem {
    private String key;
    private long offset;

    public IndexItem(String key, long offset) {
        this.key = key;
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public Long getOffset() {
        return offset;
    }
}
