package org.bull.stone;

import com.google.common.util.concurrent.FutureCallback;
import org.bull.stone.common.StoneConstants;
import org.bull.stone.file.FileMerger;
import org.bull.stone.impl.DefaultMemTable;
import org.bull.stone.impl.DefaultXFileReader;
import org.bull.stone.util.ConfigUtils;
import org.bull.stone.util.FileUtils;
import org.bull.stone.util.ShellUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-2 下午12:49
 */
public class StoneDB {
    private static final Logger LOGGER = LoggerFactory.getLogger(StoneDB.class);
    private static final String BASE_DIR = ConfigUtils.getProperties(StoneConstants.DB_PATH);
    private static final boolean DEFAULT_IS_DELTA_MERGE = true; // false 适用于全量更新索引
    private IXFileReader reader;
    private String tmpIndexDir;
    private String dbName;
    private String indexDir;


    public StoneDB(String dbName) throws IOException {
        this.dbName = dbName;
        this.tmpIndexDir = BASE_DIR + "/" + dbName + "/tmp";
        this.indexDir = BASE_DIR + "/" + dbName + "/idx";
        reader = new DefaultXFileReader(this.dbName);
    }

    public StoneDB(String dbName, int readThreadCount) throws IOException {
        this.dbName = dbName;
        this.tmpIndexDir = BASE_DIR + "/" + dbName + "/tmp";
        this.indexDir = BASE_DIR + "/" + dbName + "/idx";
        reader = new DefaultXFileReader(this.dbName, readThreadCount);
    }


    /**
     * @param key
     * @return
     */
    public Cell get(final String key, long timeout, FutureCallback<Cell> futureCallback) throws IOException, TimeoutException {
        return reader.get(key, timeout, futureCallback);

    }

    public void truncateDB() {
        File baseDBDir = new File(BASE_DIR + "/" + dbName);
        FileUtils.deleteDir(baseDBDir);
        File index = new File(this.indexDir);
        File tmp = new File(tmpIndexDir);
        index.mkdirs();
        tmp.mkdirs();
    }

    /**
     * keep the latest index
     */
    public void clearOldIndex() {
        File dir = new File(this.indexDir);
        String[] paths = dir.list();
        String current = "";
        for (String path : paths) {
            if (path.compareTo(current) > 0) {
                current = path;
            }
        }
        for (String path : paths) {
            if (!path.equals(current)) {
                new File(dir, path).delete();
            }
        }
    }

    /**
     * build temporary index
     */
    private void buildTempIndex(String dataDir, KeyValueGenerator keyValueGenerator) throws IOException {
        /**
         * init，清空原来临时索引
         */
        File tmp = new File(tmpIndexDir);
        if (tmp.exists()) {
            FileUtils.deleteDir(tmp);
        }
        tmp.mkdirs();

        IMemTable iMemTable = new DefaultMemTable(dbName);
        File dir = new File(dataDir);
        String[] paths = dir.list();
        for (String path : paths) {
            Scanner scanner = new Scanner(new BufferedReader(new FileReader(new File(dir, path))));
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                Cell cell = keyValueGenerator.generateKeyValue(line);
                iMemTable.append(cell);
            }
            iMemTable.flush();
            scanner.close();
        }
    }

    /**
     * compact temporary indexes
     *
     * @throws java.io.IOException
     */
    private void mergeIndexFiles(String tempIndexDir) throws IOException {
        List<String> tmpIndexFiles = new ArrayList<>();
        File pathtmp = new File(tempIndexDir);

        String[] pathtmps = pathtmp.list();
        for (String item : pathtmps) {
            tmpIndexFiles.add(tempIndexDir + "/" + item);
        }

        File file = new File(this.indexDir);
        if (!file.exists()) {
            file.mkdir();
        }

        String dest = this.indexDir + "/" + System.currentTimeMillis();
        if (tmpIndexFiles.size() > 1) {
            FileMerger merger = new FileMerger(dest);
            merger.merge(tmpIndexFiles);
            LOGGER.warn(" generate a new index: " + dest);
        } else {
            if (!tempIndexDir.equals(indexDir))
                ShellUtils.executeCommand(new String[]{"/bin/bash", "-c", "cp " + tempIndexDir + "/* " + indexDir});
        }
    }

    /**
     * read a line at a time ,and then add db
     *
     * @param dataDir
     * @param keyValueGenerator
     * @throws java.io.IOException
     */
    public void buildIndexByTextFiles(String dataDir, KeyValueGenerator keyValueGenerator) throws IOException {

        buildIndexByTextFiles(dataDir, keyValueGenerator, DEFAULT_IS_DELTA_MERGE);
    }

    /**
     * @param dataDir
     * @param keyValueGenerator
     * @param deltaMerge
     * @throws IOException
     */
    public void buildIndexByTextFiles(String dataDir, KeyValueGenerator keyValueGenerator, boolean deltaMerge) throws IOException {

        /**
         * do some check
         */
        File dataPath = new File(dataDir);
        String[] dataFiles = dataPath.list();
        if (dataFiles.length < 1) {
            return;
        }

        buildTempIndex(dataDir, keyValueGenerator);
        mergeIndexFiles(tmpIndexDir);
        if (deltaMerge) {
            mergeIndexFiles(indexDir);
        }
        FileUtils.deleteChildrenFiles(new File(tmpIndexDir));
    }

    public void reOpenReader() throws IOException {
        reader.reOpenReader();
        clearOldIndex();
    }
}
