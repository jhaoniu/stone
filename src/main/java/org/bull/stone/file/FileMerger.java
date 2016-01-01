package org.bull.stone.file;


import org.bull.stone.Cell;
import org.bull.stone.IXFileScanner;
import org.bull.stone.IXFlieWriter;
import org.bull.stone.impl.DefaultXFileScanner;
import org.bull.stone.impl.DefaultXFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author jhaoniu
 * @description compact several index file to one  for read!
 * @date 15-12-4 下午11:36
 */
public class FileMerger {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileMerger.class);

    private PriorityQueue<IXFileScanner> heap;
    private IXFileScanner current;
    private IXFlieWriter writer;


    public FileMerger(String destPath) throws IOException {
        writer = new DefaultXFileWriter(destPath);

    }

    public void merge(List<String> tmpIdxFiles) throws IOException {
        this.heap = new PriorityQueue<>(tmpIdxFiles.size(), new Comparator<IXFileScanner>() {
            @Override
            public int compare(IXFileScanner o1, IXFileScanner o2) {
                try {
                    int result = o1.peek().getKey().compareTo(o2.peek().getKey());
                    if (result != 0)
                        return result;
                    else
                        return Long.valueOf(o1.peek().getTs() - o2.peek().getTs()).intValue();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                    return -1;
                }
            }
        });
        for (String tmpIdxPath : tmpIdxFiles) {
            heap.add(new DefaultXFileScanner(tmpIdxPath));
        }
        String lastAppendKey = "";

        /** use heap to compact arrays */
        current = heap.poll();
        while (current != null) {
            Cell cell = current.next();
            /**  left newest record */
            if (lastAppendKey.compareTo(cell.getKey()) != 0) {
                writer.append(cell);
                lastAppendKey = cell.getKey();
            }
            if (current.peek() != null) {
                heap.add(current);
            } else {
                /** close handle */
                current.close();
            }
            current = heap.poll();
        }
        writer.close();
    }
}
