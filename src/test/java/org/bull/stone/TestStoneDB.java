package org.bull.stone;

import com.alibaba.fastjson.JSON;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-6 下午10:02
 */
public class TestStoneDB {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestStoneDB.class);

    private StoneDB stoneDB;
    private static final String stx = String.valueOf('\u0002');
    private static final String dataPath = "/opt/tm";
    private AtomicLong cnt = new AtomicLong(0);
    private AtomicLong errcnt = new AtomicLong(0);
    private AtomicLong time = new AtomicLong(0);
    private AtomicLong newtime = new AtomicLong(0);
    private AtomicLong setime = new AtomicLong(0);


    @Before
    public void setup() throws IOException {
        stoneDB = new StoneDB("poi");
    }

    //    @Test
    public void testDB() throws IOException, TimeoutException, InterruptedException {
        stoneDB.buildIndexByTextFiles(dataPath, new KeyValueGenerator() {
            private Kryo kryo = new Kryo();

            @Override
            public Cell generateKeyValue(String line) {

                String[] values = line.split("\\t");
                String key = values[0].trim();
                Set<String> value = new HashSet<>(Arrays.asList(values[1].split(stx)));
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                Output output = new Output(baos);
                kryo.writeObject(output, value);
                output.close();
                return new Cell(key, baos.toByteArray(), System.currentTimeMillis());
            }
        }, false);
        stoneDB.reOpenReader();

        File file = new File(dataPath);

        String[] paths = file.list();
        for (String path : paths) {
            newJob(new File(dataPath + "/" + path));
        }

        Thread.sleep(1000 * 60);

        System.out.println("time = " + time);
        System.out.println("cnt = " + cnt);
        System.out.println("mean = " + (time.longValue() / cnt.longValue()));
        System.out.println("errcnt = " + errcnt);
        System.out.println("newtime = " + newtime);
        System.out.println("setime = " + setime);
    }

    private void newJob(File file) throws FileNotFoundException {
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {

                String line;
                while ((line = reader.readLine()) != null) {
                    show(line);
                }
                return null;
            }
        });
    }

    @Test
//    @Ignore
    public void testCase() throws IOException, TimeoutException, InterruptedException {
        stoneDB.reOpenReader();
        get("9730");
        get("45223");
        get("46719");
        get("47671");
        get("47943");
        get("48215");
        get("48351");
        get("48487");
        get("48623");
        get("48759");
        get("49167");
        get("49439");
        get("50391");
        get("51343");
        get("51887");
        get("52023");
        get("52839");
        get("53519");
    }

    private void get(final String key) throws IOException, TimeoutException {
        final Long start = System.currentTimeMillis();
        stoneDB.get(key, 500, new FutureCallback<Cell>() {
            @Override
            public void onSuccess(Cell result) {
                long cost = System.currentTimeMillis() - start;
                Input input = new ByteBufferInput(result.getValue());
                Kryo kryo = new Kryo();
                Set<String> set = kryo.readObject(input, HashSet.class);
                System.out.println("cost = " + cost + " size:" + set.size() );//+ " result:" + JSON.toJSONString(set));
            }

            @Override
            public void onFailure(Throwable t) {

            }
        });
    }


    private void show(final String line) throws IOException, TimeoutException {

        String[] params = line.split("\\t");
        final String key = params[0];
        final int realCnt = Integer.valueOf(params[2]);


        final Long start = System.currentTimeMillis();
        stoneDB.get(key, 1000, new FutureCallback<Cell>() {
            @Override
            public void onSuccess(Cell result) {
                /**
                 * 校验逻辑
                 */
                long cost = System.currentTimeMillis() - start;
                time.addAndGet(cost);
                cnt.incrementAndGet();
                Long ss = System.currentTimeMillis();
                Kryo kryo = new Kryo();
                newtime.addAndGet((System.currentTimeMillis() - ss));
                ss = System.currentTimeMillis();
                Input input = new ByteBufferInput(result.getValue());
                Set<String> set = kryo.readObject(input, HashSet.class);
                setime.addAndGet((System.currentTimeMillis() - ss));
                if (realCnt != set.size()) {
                    System.out.println("Error Key: " + key);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                t.printStackTrace();
                errcnt.incrementAndGet();
            }
        });
    }
}
