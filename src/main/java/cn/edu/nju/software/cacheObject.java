package cn.edu.nju.software;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by zr on 2017/11/1.
 */
public class cacheObject implements Serializable{
    String desPath;
    String key;
    Map<String,String> value;
public cacheObject(String d,String k,Map<String,String> v){
    desPath = d;
    key = k;
    value = v;
}
    public String getDesPath() {
        return desPath;
    }

    public void setDesPath(String desPath) {
        this.desPath = desPath;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Map<String, String> getValue() {
        return value;
    }

    public void setValue(Map<String, String> value) {
        this.value = value;
    }
}
