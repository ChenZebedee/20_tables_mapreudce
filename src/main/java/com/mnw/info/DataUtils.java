package com.mnw.info;

import java.util.List;

/**
 * Created by shaodi.chen on 2018/10/11.
 */
public class DataUtils {
    public static String maxString(String str1, String str2){
        if (str1.compareTo(str2)>0){
            return str1;
        }else{
            return str2;
        }
    }

    public static WideTableWritable maxOrderSn(List<WideTableWritable> orderSnList){
        if (orderSnList.size()==1){
            return orderSnList.get(0);
        }
        String maxStr = orderSnList.get(0).getContactOrderSn();
        int maxIndex = 0;
        for (int i=1;i<orderSnList.size();i++){
            maxStr = maxString(maxStr,orderSnList.get(i).getContactOrderSn());
            if (maxStr.equals(orderSnList.get(i).getContactOrderSn())){
                maxIndex = i;
            }
        }
        return orderSnList.get(maxIndex);
    }

    public static WideTableWritable maxSearchSN(List<WideTableWritable> machineList){
        String maxStr = machineList.get(0).getMachineSearchSN();
        int maxIndex = 0;
        for (int i=1;i<machineList.size();i++){
            maxStr = maxString(maxStr,machineList.get(i).getMachineSearchSN());
            if (maxStr.equals(machineList.get(i).getMachineSearchSN())){
                maxIndex = i;
            }
        }
        return machineList.get(maxIndex);
    }
}
