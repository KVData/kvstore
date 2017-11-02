package cn.edu.nju.software;

import cn.helium.kvstore.common.KvStoreConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
    public void SaveToHdfs(String srcP,String desP){
        Path srcpath = new Path(srcP);
        Path despath = new Path(desP);
        //连接hdfs
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsUrl), conf);
            fs.copyFromLocalFile(srcpath,despath);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void fromCacheToHdfs(String srcP){//待改进，找出目标文件重复的一起插入
        try {
            ObjectInputStream is = new ObjectInputStream(new FileInputStream(srcP));
            List<cacheObject> l = (List<cacheObject>) is.readObject();
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
                //写hdfs
                Path desP = new Path(tmp.get(0).getDesPath());
                Configuration conf = new Configuration();
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                FileSystem fs = FileSystem.get(URI.create(KvStoreConfig.getHdfsUrl()),conf);
                Map<String, Map<String, String>> tmpm;

                if (fs.createNewFile(desP)) {
                    //创建文件并且写入对象
                    tmpm = new HashMap<>();
                } else {
                    //读出object
                    ObjectInputStream ois = new ObjectInputStream((fs.open(desP)));
                    tmpm = (Map<String, Map<String, String>>) ois.readObject();
                }
                ObjectOutputStream os = new ObjectOutputStream(fs.create(desP, false));
                for(cacheObject c:tmp) {//循环加入list
                    String key = c.getKey();
                    Map<String, String> value = c.getValue();
                    tmpm.put(key,value);

                }
                os.writeObject(tmpm);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public Map<String,String> readHdfs(String des,String key){
        try{
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            Path desP = new Path(des);
            FileSystem fs = FileSystem.get( URI.create(KvStoreConfig.getHdfsUrl()),conf);
            ObjectInputStream ois = new ObjectInputStream(fs.open(desP));
            Map<String,Map<String,String>> tmp = (Map<String,Map<String,String>>)ois.readObject();
            return tmp.get(key);
        }catch (Exception e){
            //
        }
        return null;
    }
    public void toCache(){

    }
}
