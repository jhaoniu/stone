package org.bull.stone.impl;


import org.bull.stone.Cell;
import org.bull.stone.IXFileScanner;
import org.bull.stone.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-5 上午9:56
 */
public class DefaultXFileScanner implements IXFileScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultXFileScanner.class);

    private static final int DEFAULT_BLOCK_PER_LOAD = 5;

    private IndexItem[] indexItems;
    private RandomAccessFile scanner;
    private long indexOffset;
    private int currentIndex;
    private DataInputStream dataInputStream;

    public DefaultXFileScanner(String path) throws IOException {
        loadIndex(path);
        currentIndex = 0;
        scanner = new RandomAccessFile(path, "r");
        scanner.seek(0);
    }

    @Override
    public Cell peek() throws IOException {
        if (this.dataInputStream == null || this.dataInputStream.available() == 0) {
            this.dataInputStream = loadBlock();
        }
        if (this.dataInputStream != null) {
            this.dataInputStream.mark(1);
            int keyLength = this.dataInputStream.readInt();
            int valueLength = this.dataInputStream.readInt();
            byte[] keyBytes = new byte[keyLength];
            this.dataInputStream.read(keyBytes);
            String row = Bytes.toString(keyBytes);
            byte[] value = new byte[valueLength];
            this.dataInputStream.read(value);
            Long ts = this.dataInputStream.readLong();
            Cell cell = new Cell(row, value, ts);
            this.dataInputStream.reset();
            return cell;
        }
        return null;
    }

    @Override
    public Cell next() throws IOException {
        if (this.dataInputStream == null || this.dataInputStream.available() == 0) {
            this.dataInputStream = loadBlock();
            /**
             * 如果load不到
             */
            if (this.dataInputStream == null) {
                return null;
            }
        }
        int keyLength = this.dataInputStream.readInt();
        int valueLength = this.dataInputStream.readInt();
        byte[] keyBytes = new byte[keyLength];
        this.dataInputStream.read(keyBytes);
        String row = Bytes.toString(keyBytes);
        byte[] value = new byte[valueLength];
        this.dataInputStream.read(value);
        Long ts = this.dataInputStream.readLong();
        Cell cell = new Cell(row, value, ts);
        return cell;
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    /**
     * load 5 block at a time
     *
     * @return
     * @throws IOException
     */

    private DataInputStream loadBlock() throws IOException {
        if (currentIndex == indexItems.length) {
            return null;
        }
        scanner.seek(indexItems[currentIndex].getOffset());
        /**
         * compute blockLength
         */
        int blockLength;
        if (indexItems.length - 1 - currentIndex >= DEFAULT_BLOCK_PER_LOAD) {
            blockLength = Long.valueOf(indexItems[currentIndex + DEFAULT_BLOCK_PER_LOAD].getOffset() - indexItems[currentIndex].getOffset()).intValue();
            currentIndex += DEFAULT_BLOCK_PER_LOAD;
        } else {
            blockLength = Long.valueOf(this.indexOffset - indexItems[currentIndex].getOffset()).intValue();
            currentIndex = indexItems.length;
        }
        FileChannel inChannel = scanner.getChannel();
        ByteBuffer buf = ByteBuffer.allocate(blockLength);
        /** read into buffer.*/
        int readBytes = inChannel.read(buf);
        if (readBytes < blockLength) {
            LOGGER.error("readBytes less than blockLength");
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
        DataInputStream dataInputStream = new DataInputStream(bais);
        return dataInputStream;
    }

    private void loadIndex(String path) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r");
        long fileSize = randomAccessFile.length();
        randomAccessFile.seek(fileSize - 8 - 4);
        this.indexOffset = randomAccessFile.readLong();
        int totalIndex = randomAccessFile.readInt();
        randomAccessFile.seek(this.indexOffset);
        indexItems = new IndexItem[totalIndex];
        for (int i = 0; randomAccessFile.getFilePointer() != (fileSize - 8 - 4); i++) {
            int length = randomAccessFile.readInt();
            byte[] keyBytes = new byte[length];
            randomAccessFile.read(keyBytes);
            String key = Bytes.toString(keyBytes);
            long offset = randomAccessFile.readLong();
            indexItems[i] = new IndexItem(key, offset);
        }
        randomAccessFile.close();
    }
}
