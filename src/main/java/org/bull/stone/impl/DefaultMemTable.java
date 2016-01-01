package org.bull.stone.impl;


import org.bull.stone.Cell;
import org.bull.stone.HeapSize;
import org.bull.stone.IMemTable;
import org.bull.stone.IXFlieWriter;
import org.bull.stone.common.StoneConstants;
import org.bull.stone.util.ConfigUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jhaoniu
 * @description an IMemTable implementation
 * @date 15-10-23 下午5:56
 */
public class DefaultMemTable implements IMemTable, HeapSize {

    private final AtomicLong size;
    private final static int MAX_MEMTABLE_SIZE = 1024 * 1024 * 512;
    private String dbname;

    public DefaultMemTable(String dbname) {
        this.size = new AtomicLong(0);
        memTable = new ConcurrentSkipListMap<>();
        this.dbname = dbname;
    }

    private ConcurrentSkipListMap<String, Cell> memTable;

    @Override
    public void append(Cell cell) throws IOException {
        if (cell == null) {
            return;
        }

        memTable.put(cell.getKey(), cell);
        /** size 修改 */
        size.addAndGet(cell.heapSize());

        if (this.heapSize() > MAX_MEMTABLE_SIZE) {
            flush();
        }
    }

    @Override
    public Cell get(String key) {
        return memTable.get(key);
    }

    @Override
    public void flush() throws IOException {
        /**
         * 将临时索引写入临时文件夹
         */
        IXFlieWriter ixFlieWriter = new DefaultXFileWriter(ConfigUtils.getProperties(StoneConstants.DB_PATH) + "/" + dbname + "/tmp/" + System.currentTimeMillis());
        Set<Map.Entry<String, Cell>> entrySet = memTable.entrySet();
        Iterator<Map.Entry<String, Cell>> iterator = entrySet.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Cell> entry = iterator.next();
            ixFlieWriter.append(entry.getValue());
        }
        ixFlieWriter.close();
        memTable.clear();
        size.set(0);
    }

    @Override
    public long heapSize() {
        //TODO heapSize 计算
        return size.get();
    }
}
