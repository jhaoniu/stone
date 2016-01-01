# stone
## A simple kv db
## 使用Kryo 序列化 demo

```java
StoneDB stoneDB = new StoneDB("dbName");
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
Cell result = stoneDB.get(key, 1000, new FutureCallback<Cell>() {
    @Override
    public void onSuccess(Cell result) {
        //TODO
    }

    @Override
    public void onFailure(Throwable t) {
        //TODO
    }
});
Input input = new ByteBufferInput(result.getValue());
Kryo kryo = new Kryo();
Set<String> set = kryo.readObject(input, HashSet.class);

```
