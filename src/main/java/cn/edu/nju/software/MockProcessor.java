package cn.edu.nju.software;

import cn.helium.kvstore.processor.Processor;
import java.util.HashMap;
import java.util.Map;

public class MockProcessor implements Processor {
    private Map<String, Map<String, String>> store = new HashMap<>();

    @Override
    public Map<String, String> get(String key) {
        Map<String, String> defaultValue = new HashMap<>();
        defaultValue.put("no key find", key);
        return store.getOrDefault(key, defaultValue);
    }

    @Override
    public boolean put(String key, Map<String, String> value) {
        store.put(key, value);
        return true;
    }

    @Override
    public boolean batchPut(Map<String, Map<String, String>> records) {
        store.putAll(records);
        return true;
    }

    @Override
    public byte[] process(byte[] input) {
        System.out.println(String.format("receive info: %s", new String(input)));
        return "received!".getBytes();
    }
}
