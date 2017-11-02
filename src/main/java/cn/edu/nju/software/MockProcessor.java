package cn.edu.nju.software;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;

import java.io.*;
import java.util.ArrayList;

import java.util.Map;

public class MockProcessor implements Processor {
    private KeyMapUtil keyUtil = new KeyMapUtil();
    private int podId;
    private ArrayList<cacheObject> cacheList = null;
    private HdfsUtil hdfsUtil = new HdfsUtil();
    private HelpUtil helpUtil = new HelpUtil();
    private static String cacheRoot = "/opt/localdisk/cache";
    private static String hdfsRoot = "/test/";

    public MockProcessor(){

    }
    @Override
    public Map<String, String> get(String key) {
        podId = RpcServer.getRpcServerId();
        System.out.println("podID:"+podId);
        //先去缓存中读
        int podNum = KvStoreConfig.getServersNum();

        try{
//            File cacheFile = new File("/opt/localdisk/cachePod"+podId);
            File cacheFile = new File(cacheRoot+podId);
            ObjectInputStream is = new ObjectInputStream(new FileInputStream(cacheFile));
            cacheList = (ArrayList<cacheObject>)is.readObject();
            for(int i=0;i<cacheList.size();i++){
                cacheObject c = cacheList.get(i);
                if(c.getKey().equals(key)){
                    return c.getValue();
                }
            }
            //去其他的机器上读,inform
            try {
                for(int i = 0;i<podNum;i++) {
                    if(i!=podId) {
                        String info = "read:" + key;
                        byte[] response = RpcClientFactory.inform(i, info.getBytes());
                        String msg = response.toString().split(":")[0];
                        if (msg.equals("hasdata")) {
                            String value = response.toString().split(":")[1];
                            return helpUtil.toMap(value);
                        }
                    }
                }
//                byte[] secondresponse = RpcClientFactory.inform(sopodId,info.getBytes());
//                //处理返回值
//                msg = secondresponse.toString().split(":")[0];
//                if(msg.equals("hasdata")){
//                    String value = secondresponse.toString().split(":")[1];
//                    return helpUtil.toMap(value);
//                }
            }catch (IOException e){
               System.out.println("connect error");
            }
            //读hdfs
            String index = keyUtil.hashKey(key);
            String fileName = hdfsRoot+index;//需要指定路径
            Map<String,String> res = hdfsUtil.readHdfs(fileName,key);
            if(res!=null){
                return res;
            }else{
                return null;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean put(String key, Map<String, String> value) {
        podId = RpcServer.getRpcServerId();
        System.out.println("podID:"+podId);
        //对key值做hash映射
        String index = keyUtil.hashKey(key);
        //映射完成之后写进缓存,写进另外两个kvpod的缓存中
        //String desFile = "opt/localdisk/"+index;
        String fileName = hdfsRoot+index;
        File cacheFile = new File(cacheRoot+podId);
        if(!cacheFile.exists()){
            try {
                cacheFile.createNewFile();
                cacheList = new ArrayList<cacheObject>();
                cacheObject c = new cacheObject(fileName,key,value);
                cacheList.add(c);
                ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(cacheFile));
                os.writeObject(cacheList);
                return true;
            }catch(Exception e){
                e.printStackTrace();
                return false;
            }
        }
        try {
            //读取cacheList
            ObjectInputStream is = new ObjectInputStream(new FileInputStream(cacheFile));
            cacheList = (ArrayList<cacheObject>)is.readObject();
            cacheObject c = new cacheObject(fileName,key,value);
            cacheList.add(c);
            ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(cacheFile));
            os.writeObject(cacheList);
            if(cacheList.size()>=1000){
                //将之前的cache写入hdfs
                hdfsUtil.fromCacheToHdfs(cacheRoot+podId);
                //序列化写入cache
            }
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        //缓存写满一次存入hdfs并清空缓存
        //考虑在本机上做缓存的问题
        return true;
    }

    /**
     * 需要改写批量处理，一批数据一起直接写入缓存和hdfs
    * */
    @Override
    public boolean batchPut(Map<String, Map<String, String>> records) {
        podId = RpcServer.getRpcServerId();
        System.out.println("podID:"+podId);
        for(Map.Entry<String,Map<String,String>> entry:records.entrySet()){
            put(entry.getKey(),entry.getValue());
        }
        return true;
    }

    @Override
    public byte[] process(byte[] input) {//通信
        podId = RpcServer.getRpcServerId();
        System.out.println("podID:"+podId);
        String info = input.toString();
        String command = info.split(":")[0];
        String value = info.split(":")[0];
        if(command.equals("read")){//读取本地缓存
            try{
                File cacheFile = new File(cacheRoot+podId);
                ObjectInputStream is = new ObjectInputStream(new FileInputStream(cacheFile));
                cacheList = (ArrayList<cacheObject>)is.readObject();
                for(int i=0;i<cacheList.size();i++){
                    cacheObject c = cacheList.get(i);
                    if(c.getKey().equals(value)){
                        //return c.getValue();
                        String res = "hasdata:"+c.getValue().toString();
                        return res.getBytes();
                    }
                }
                return "nodata".getBytes();
            }catch (Exception e){
                return  "error".getBytes();
            }
        }
        return "nohandle".getBytes();
    }
}
