package org.bull.stone.impl;

import com.google.common.util.concurrent.*;

import org.bull.stone.Cell;
import org.bull.stone.IXFileReader;
import org.bull.stone.cache.Cache;
import org.bull.stone.cache.ConcurrentLRUCache;
import org.bull.stone.common.StoneConstants;
import org.bull.stone.util.Bytes;
import org.bull.stone.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.*;

/**
 * @author jhaoniu
 * @description thread safe
 * @date 15-12-4 下午9:59
 */
public class DefaultXFileReader implements IXFileReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultXFileReader.class);

    private IndexItem[] indexItems;
    private static final String BASE_DIR = ConfigUtils.getProperties(StoneConstants.DB_PATH);
    private String indexDir;
    private long indexOffset;

    private ThreadLocal<RandomAccessFile> threadLocalReaders;
    private BlockingQueue<RandomAccessFile> readersQueue;
    private BlockingQueue<RandomAccessFile> lastReadersQueue;
    private ListeningExecutorService service;
    private static final Integer DEFAULT_MAX_THREAD_CNT = 8;
    private int readThreadCount;
    private static final int DEFAULT_CACHE_SIZE = 1024;
    private Cache<String, Cell> cache;

    public DefaultXFileReader(String dbName, int readThreadCount, int cacheSize) throws IOException {
        this.readThreadCount = readThreadCount;
        this.indexDir = BASE_DIR + "/" + dbName + "/idx";
        this.threadLocalReaders = new ThreadLocal<>();
        this.cache = new ConcurrentLRUCache<>(cacheSize, Double.valueOf(cacheSize * 0.9).intValue());
        this.service = createListeningExecutorService(this.readThreadCount);
        this.readersQueue = new LinkedBlockingQueue<>();
    }

    public DefaultXFileReader(String dbName, int readThreadCount) throws IOException {
        this(dbName, readThreadCount, DEFAULT_CACHE_SIZE);
    }

    private ListeningExecutorService createListeningExecutorService(final int readThreadCount) {
        return MoreExecutors.listeningDecorator(new ThreadPoolExecutor(readThreadCount, readThreadCount,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()) {
            @Override
            protected void terminated() {
                for (RandomAccessFile item : lastReadersQueue) {
                    try {
                        item.close();
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
            }
        });
    }

    public DefaultXFileReader(String dbName) throws IOException {
        this(dbName, DEFAULT_MAX_THREAD_CNT, DEFAULT_CACHE_SIZE);
    }

    @Override
    public Cell get(final String key, long timeout, FutureCallback<Cell> futureCallback) throws TimeoutException {

        ListenableFuture<Cell> future = service.submit(new Callable<Cell>() {
            @Override
            public Cell call() throws Exception {
                RandomAccessFile reader = threadLocalReaders.get();
                if (reader == null) {
                    reader = getCurrentReader();
                    readersQueue.add(reader);
                    threadLocalReaders.set(reader);
                }
                return get(key, reader);
            }
        });
        Futures.addCallback(future, futureCallback);
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return null;

    }

    @Override
    public void reOpenReader() throws IOException {
        String current = getCurrentReaderPath();
        if (null != current) {
            freshIndex(getCurrentReaderPath());
            lastReadersQueue = readersQueue;
            readersQueue = new LinkedBlockingQueue<>();
            ListeningExecutorService lastService = this.service;
            this.service = createListeningExecutorService(this.readThreadCount);
            lastService.shutdown();
        }
        this.cache.clear();
    }


    private RandomAccessFile getCurrentReader() throws IOException {
        return new RandomAccessFile(this.getCurrentReaderPath(), "r");
    }

    /**
     * return newest index path
     *
     * @return
     * @throws java.io.IOException
     */
    private String getCurrentReaderPath() throws IOException {
        File dir = new File(indexDir);
        String[] paths = dir.list();
        String current = "";
        for (String path : paths) {
            if (path.compareTo(current) > 0) {
                current = path;
            }
        }
        if (current.equals(""))
            return null;
        return indexDir + "/" + current;
    }


    private Cell get(String key, RandomAccessFile reader) throws IOException {

        if (cache.get(key) == null) {
            int index = binaryLowSearch(indexItems, key);
            if (index < 0) {
                return null;
            }
            /**
             * load block
             */
            reader.seek(indexItems[index].getOffset());
            int blockLength;
            if (index < indexItems.length - 1)
                blockLength = Long.valueOf(indexItems[index + 1].getOffset() - indexItems[index].getOffset()).intValue();
            else
                blockLength = Long.valueOf(this.indexOffset - indexItems[index].getOffset()).intValue();

            FileChannel inChannel = reader.getChannel();
            ByteBuffer buf = ByteBuffer.allocate(blockLength);
            /** read into buffer.*/
            int readBytes = inChannel.read(buf);
            if (readBytes < blockLength) {
                LOGGER.error(" readBytes less than blockLength");
                return null;
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
            DataInputStream dataInputStream = new DataInputStream(bais);
            String row;
            do {
                int keyLength = dataInputStream.readInt();
                int valueLength = dataInputStream.readInt();
                byte[] keyBytes = new byte[keyLength];
                dataInputStream.read(keyBytes);
                row = Bytes.toString(keyBytes);
                byte[] value = new byte[valueLength];
                dataInputStream.read(value);
                Long ts = dataInputStream.readLong();
                if (row.equals(key)) {
                    Cell cell = new Cell(key, value, ts);
                    /**
                     * cache data
                     */
                    cache.put(key, cell);
                    return cell;
                }
            }
            while (row.compareTo(key) < 0 && bais.available() > 0);
        }
        return cache.get(key);
    }


    private void freshIndex(String path) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r");
        long fileSize = randomAccessFile.length();
        randomAccessFile.seek(fileSize - 8 - 4);
        long newIndexOffset = randomAccessFile.readLong();
        int totalIndex = randomAccessFile.readInt();
        randomAccessFile.seek(newIndexOffset);
        IndexItem[] newIndexItems = new IndexItem[totalIndex];
        for (int i = 0; randomAccessFile.getFilePointer() != (fileSize - 8 - 4); i++) {
            int length = randomAccessFile.readInt();
            byte[] keyBytes = new byte[length];
            randomAccessFile.read(keyBytes);
            String key = Bytes.toString(keyBytes);
            long offset = randomAccessFile.readLong();
            newIndexItems[i] = new IndexItem(key, offset);
        }
        indexItems = newIndexItems;
        indexOffset = newIndexOffset;
        randomAccessFile.close();
    }


    private int binaryLowSearch(IndexItem[] indexItems, String key) {
        int low = 0;
        int high = indexItems.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = key.compareTo(indexItems[mid].getKey());
            if (cmp > 0)
                low = mid + 1;
            else if (cmp < 0)
                high = mid - 1;
            else
                return mid;
        }
        return low - 1;
    }
}
