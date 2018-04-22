package com.huawei.cn;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class Util {
    public static String getRegionNo(String caller,String time){
        String subtime=time.substring(0,6);

        int hash=(caller+subtime).hashCode();
        hash=(hash&Integer.MAX_VALUE)%100;
        DecimalFormat decimalFormat=new DecimalFormat();
        decimalFormat.applyPattern("00");
        String ans=decimalFormat.format(hash);
        return  ans;

    }
}
