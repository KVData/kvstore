package cn.edu.nju.software;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zr on 2017/11/2.
 */
public class HelpUtil {
    public Map<String,String> toMap(String singInfo){
        String str1 = singInfo.replaceAll("\\{|\\}","");
        String str2 = str1.replaceAll(" ","");
        String str3 = str2.replaceAll(",","&");

        Map<String,String> map = null;
        if((str3!=null)&&(!str3.trim().equals(""))){
            String[] resArray = str3.split("&");
            if(resArray.length!=0){
                map = new HashMap<>();
                for(String arrayStr:resArray){
                  if((null!=arrayStr)&&(!"".equals(arrayStr.trim()))) {
                        int index=arrayStr.indexOf("=");
                        if(-1!=index){
                        map.put(arrayStr.substring(0,index),arrayStr.substring(index+1));
                        }
                    }
                }
            }
        }
        return map;
    }

    public String mapToString(String key,Map<String,String> value){
        String res=key+";";
        String v = value.toString()+"\n";
        res+=v;
        return res;
    }
    public String cacheToString(cacheObject c){
        String des = c.getDesPath();
        String key = c.getKey();
        Map<String,String> value = c.getValue();
        String res=des+";"+key+";"+value.toString()+"\n";
        return res;
    }
}
