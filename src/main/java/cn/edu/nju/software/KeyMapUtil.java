package cn.edu.nju.software;

/**
 * 对Key值做映射
 * Created by zr on 2017/10/31.
 */
public class KeyMapUtil {
    public String hashKey(String key){
        String res = "";
        int prime = 20;
        res = String.valueOf(additiveHash(key,prime));
        return res;
    }
    public int FNVHash1(String data)//乘法hash
    {
        final int p = 16777619;
        int hash = (int)2166136261L;
        for(int i=0;i<data.length();i++) {
            hash = (hash ^ data.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return hash;
    }
    public int additiveHash(String key,int prime)//加法hash
    {
        int hash,i;
        for(hash=key.length(),i=0;i<key.length();i++) {
            hash += key.charAt(i);
        }
        return(hash%prime);
    }
}
