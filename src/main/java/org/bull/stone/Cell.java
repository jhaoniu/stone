package org.bull.stone;

/**
 * @author jhaoniu
 * @description key value simple design
 * @date 15-12-1 下午1:48
 */
public class Cell implements HeapSize {
    private String key;
    private byte[] value;
    private Long ts;


    public Cell(String key, byte[] value, Long ts) {
        this.key = key;
        this.value = value;
        this.ts = ts;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public long heapSize() {
        return key.getBytes().length + value.length + 8;
    }
}
