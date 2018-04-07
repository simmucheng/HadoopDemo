package com.huawei.cn;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperTest {
    @Test
    public void ls() throws Exception {
        ZooKeeper zk=new ZooKeeper("192.168.0.110:2181，192.168.0.113：2181",5000,null);
        List<String>list = zk.getChildren("/",null);
        for(String tt:list){
            System.out.println(tt);
        }

    }

    /**
     * 递归列出所有子节点的信息
     */
    @Test
    public void lsall() throws Exception {
        ls("/");
    }

    public void ls(String path) throws Exception {
        ZooKeeper zk=new ZooKeeper("192.168.0.110:2181",5000,null);
        List<String>list=zk.getChildren(path,null);
        if(list==null||list.isEmpty()){
            return ;
        }
        if(path.equals("/")){
            path="";
        }
        for(String tt:list){
            String next=path+"/"+tt;
            System.out.println(tt);
            ls(next);
        }

    }

    /**
     * 只能处理一次的watches
     * @throws Exception
     */
    @Test
    public void watchOnceTest() throws Exception {
        ZooKeeper zk=new ZooKeeper("192.168.0.110:2181",5000,null);
        Watcher w=null;
        //每次watch注册后，数据修改后只会调用一次，调完就失效
        w=new Watcher() {
            //回调函数
            public void process(WatchedEvent event) {
                System.out.println("data is changing");

            }
        };
        //stat是返回该路径的相关信息
        zk.getData("/c",w,null);
        while(true){
            Thread.sleep(5000);
        }
    }

    /**
     * 能无限次处理的watches
     * @throws Exception
     */
    @Test
    public void watchManyTest() throws Exception {
        final ZooKeeper zk=new ZooKeeper("192.168.0.110:2181",5000,null);
        Watcher w=null;
        //每次watch注册后，数据修改后只会调用一次，调完就失效
        w=new Watcher() {
            //回调函数
            public void process(WatchedEvent event) {
                System.out.println("data is changing");
                //this 在这里因为是在Watch的调用中，所以this=w
                try {
                    zk.getData("/c",this,null);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        //stat是返回该路径的相关信息
        zk.getData("/c",w,null);
        while(true){
            Thread.sleep(5000);
        }
    }
}
