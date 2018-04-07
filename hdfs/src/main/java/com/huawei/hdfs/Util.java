package com.huawei.hdfs;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Util {
    public static String getInfo(Object o,String msg){
        return getHostname()+":"+getPID()+":"+getTID()+":"+getObjInfo(o)+":"+msg;

    }

    /**
     * get hostname
     */
    public static String getHostname(){
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @return
     */
    public static int getPID(){
        String info = ManagementFactory.getRuntimeMXBean().getName();
        return Integer.parseInt(info.substring(0,info.indexOf("@")));
    }

    /**
     *
     * @return
     */
    public static String getTID(){
        return Thread.currentThread().getName();
    }

    /**
     *
     * @param o
     * @return
     */
    public static String getObjInfo(Object o){
        //如果不是简单名称，那么会带有包名
        try {
            String name =o.getClass().getSimpleName();
            return name+"@"+o.hashCode();
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

}
