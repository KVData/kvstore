package cn.edu.nju.software;

import cn.helium.kvstore.common.KvStoreConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;




/**
 * Created by zr on 2017/10/31.
 */
public class HdfsUtil {
    String hdfsUrl = KvStoreConfig.getHdfsUrl();
    HelpUtil helpUtil = new HelpUtil();
    public void SaveToHdfs(String srcP,String desP){
        Path srcpath = new Path(srcP);
        Path despath = new Path(desP);
        //连接hdfs
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsUrl), conf);
            fs.copyFromLocalFile(srcpath,despath);
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void fromCacheToHdfs(ArrayList<cacheObject> cacheList)throws Exception{//待改进，找出目标文件重复的一起插入
            List<cacheObject> l = cacheList;
            for(int i=0;i<l.size();i++) {
                List<cacheObject> tmp = new ArrayList<>();
                tmp.add(l.get(i));
                String standardPaht = l.get(i).getDesPath();
                for(int j=1;j<l.size();j++){//找出重复路径的元素
                    if(l.get(j).getDesPath().equals(standardPaht)){
                        tmp.add(l.get(j));
                        l.remove(j);
                        j--;
                    }
                }
                l.remove(i);
                i--;

                //改成string格式，不要序列化
                //写hdfs
                Path desP = new Path(tmp.get(0).getDesPath());
                Path lockP = new Path(tmp.get(0).getDesPath()+"_inuse");
                Configuration conf = new Configuration();
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                FileSystem fs = FileSystem.get(URI.create(KvStoreConfig.getHdfsUrl()),conf);
                if (fs.createNewFile(desP)) {

                } else {

                }
                //文件锁
                while(true) {
                    if (fs.createNewFile(lockP)) {
                        FSDataOutputStream fo = fs.append(desP);
                        for (cacheObject c : tmp) {//循环加入list
                            String res = helpUtil.mapToString(c.getKey(), c.getValue());
                            fo.writeBytes(res);
                        }
                        fo.close();
                        fs.delete(lockP,true);
                        break;
                    }else{
                        System.out.println("in use...");
                        Thread.sleep(30);
                    }
                }
                fs.close();
            }
    }
    public Map<String,String> readHdfs(String des,String key){
        try{
            System.out.println("des:"+des);
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            Path desP = new Path(des);
            FileSystem fs = FileSystem.get( URI.create(KvStoreConfig.getHdfsUrl()),conf);
            FSDataInputStream fi = fs.open(desP);
            InputStreamReader is = new InputStreamReader(fi);
            BufferedReader br = new BufferedReader(is);
            String value="";
            while((value=br.readLine())!=null){
                String[] tmps = value.split(";");
                String k=tmps[0];
                String v=tmps[1];
                if(k.equals(key)){
                    br.close();
                    is.close();
                    fi.close();
                    return helpUtil.toMap(v);
                }
            }
            br.close();
            is.close();
            fi.close();
            return null;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public void toCache(){

    }
}
