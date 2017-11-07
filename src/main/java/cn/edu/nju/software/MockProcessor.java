package cn.edu.nju.software;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;

import java.io.*;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;

public class MockProcessor implements Processor {
    private KeyMapUtil keyUtil = new KeyMapUtil();
    private int podId;
    private  ArrayList<cacheObject> cacheList = null;
    private HdfsUtil hdfsUtil = new HdfsUtil();
    private HelpUtil helpUtil = new HelpUtil();
    private static String cacheRoot = "/home/hadoop/cachePod/cache";
    private static String hdfsRoot = "/test7/";

    public MockProcessor(){

    }
    @Override
    public int count(Map<String,String> filter){
        return 0;
    }

    @Override
    public Map<Map<String,String>,Integer>groupBy(List<String> columns){
        return null;
    }

    @Override
    public Map<String, String> get(String key) {
        synchronized (MyLock.LOCK) {
            podId = RpcServer.getRpcServerId();
            //先去缓存中读
            int podNum = KvStoreConfig.getServersNum();
            try {
                File cacheFile = new File(cacheRoot + podId);
                FileReader fr = new FileReader(cacheFile);
                BufferedReader br = new BufferedReader(fr);
                String tmpvalue = "";
                while((tmpvalue=br.readLine())!=null){
                    String[] tmps = tmpvalue.split(";");
                    if(tmps[1].equals(key)){
                        br.close();
                        fr.close();
                        return helpUtil.toMap(tmps[2]);
                    }
                }
                br.close();
                fr.close();
                //去其他的机器上读,inform
                for (int i = 0; i < podNum; i++) {
                        if (i != podId) {
                            try{
                                String info = "read:" + key;
                                byte[] response = RpcClientFactory.inform(i, info.getBytes());
                                String msg = new String(response);
                                String command = msg.split(":")[0];
                                if (command.equals("hasdata")) {
                                    String value = msg.split(":")[1];
                                    return helpUtil.toMap(value);
                                }
                            }catch (IOException e){
                                e.printStackTrace();
                            }
                        }
                }
                //读hdfs
                String index = keyUtil.hashKey(key);
                String fileName = hdfsRoot + index;//需要指定路径
                Map<String, String> res = hdfsUtil.readHdfs(fileName, key);
                if (res != null) {
                    return res;
                } else {
                    return null;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    @Override
    public boolean put(String key, Map<String, String> value) {
        synchronized (MyLock.LOCK) {
            podId = RpcServer.getRpcServerId();
            //对key值做hash映射
//            System.out.println("reiceve put:"+key);
            String index = keyUtil.hashKey(key);

            String fileName = hdfsRoot + index;
            File cacheFile = new File(cacheRoot + podId);

            if (!cacheFile.exists()) {
                try {
                    cacheFile.createNewFile();
                    cacheList = new ArrayList<cacheObject>();
                    cacheObject c = new cacheObject(fileName, key, value);
                    cacheList.add(c);
//                    ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(cacheFile));
//                    os.writeObject(cacheList);
//                    os.close();
                    FileWriter fw = new FileWriter(cacheFile,true);
                    fw.append(helpUtil.cacheToString(c));
                    fw.close();
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }

            try {
                //读取cacheList

//                ObjectInputStream is = new ObjectInputStream(new FileInputStream(cacheFile));
//                cacheList = (ArrayList<cacheObject>) is.readObject();
//                is.close();
                cacheObject c = new cacheObject(fileName, key, value);
                cacheList.add(c);
                FileWriter fw = new FileWriter(cacheFile,true);
                fw.append(helpUtil.cacheToString(c));
                fw.close();
                if (cacheList.size() >= 1000) {
                    //将之前的cache写入hdfs
                    hdfsUtil.fromCacheToHdfs(cacheList);
                    //序列化写入cache
                    cacheList.clear();
                    FileWriter fwt = new FileWriter(cacheFile,false);
                    fwt.write("");
                    fw.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            //缓存写满一次存入hdfs并清空缓存
            //考虑在本机上做缓存的问题
            return true;
        }
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
        String info = new String(input);
        String command = info.split(":")[0];
        String value = info.split(":")[1];
        if(command.equals("read")){//读取本地缓存
            try{
                synchronized (MyLock.LOCK) {
                    File cacheFile = new File(cacheRoot + podId);
                    FileReader fr = new FileReader(cacheFile);
                    BufferedReader br = new BufferedReader(fr);
                    String tmpvalue = "";
                    while((tmpvalue=br.readLine())!=null){
                        String[] tmps = tmpvalue.split(";");
                        if(tmps[1].equals(value)){
//                            System.out.println("get key in cache! key:"+value);
                            String res = "hasdata:"+tmps[2];
                            return res.getBytes();
                        }
                    }
                    br.close();
                    fr.close();
                    return "nodata".getBytes();
                }
            }catch (Exception e){
                return  "error".getBytes();
            }
        }
        return "nohandle".getBytes();
    }
}
