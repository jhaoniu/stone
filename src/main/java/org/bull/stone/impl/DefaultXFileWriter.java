package org.bull.stone.impl;

import org.bull.stone.Cell;
import org.bull.stone.IXFlieWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author jhaoniu
 * @description: File Level abstract,not thread safe
 * @date 15-12-1 下午1:45
 */
public class DefaultXFileWriter implements IXFlieWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultXFileWriter.class);

    private RandomAccessFile dataWriter;
    private DataOutputStream dos;
    private ByteArrayOutputStream baosInMemory;
    private int blockThreshold = 1024 * 64;
    private int currentSize = 0;
    private String firstKey;
    private long offset;
    private String path;
    private int totalIndex;


    public DefaultXFileWriter(String path) throws IOException {
        this.path = path;
        dataWriter = new RandomAccessFile(path, "rw");
        baosInMemory = new ByteArrayOutputStream();
        dos = new DataOutputStream(baosInMemory);
        offset = 0;
        totalIndex = 0;
        LOGGER.warn("init new path:" + this.path);
    }

    @Override
    public void append(Cell cell) throws IOException {
        if (cell == null) {
            return;
        }
        /** when generate new block */
        if (currentSize == 0) {
            firstKey = cell.getKey();
            offset = dataWriter.getFilePointer();
        }

        dataWriter.writeInt(cell.getKey().getBytes().length);
        dataWriter.writeInt(cell.getValue().length);
        dataWriter.write(cell.getKey().getBytes());
        dataWriter.write(cell.getValue());
        dataWriter.writeLong(cell.getTs());
        currentSize += (4 + 4 + cell.getKey().getBytes().length + cell.getValue().length + 8);

        if (currentSize > blockThreshold) {
            finishBlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (currentSize > 0) {
            finishBlock();
        }
        long indexOffset = dataWriter.getFilePointer();
        byte[] indexMeta = baosInMemory.toByteArray();
        dataWriter.write(indexMeta);
        dataWriter.writeLong(indexOffset);
        dataWriter.writeInt(totalIndex);
        dataWriter.close();
        LOGGER.warn(" ## write file colse: " + this.path + "  indexSize:" + indexMeta.length + " totalIndex:" + totalIndex);
    }

    private void finishBlock() throws IOException {
        totalIndex += 1;
        currentSize = 0;
        dos.writeInt(firstKey.getBytes().length);
        dos.write(firstKey.getBytes());
        dos.writeLong(offset);
    }
}
