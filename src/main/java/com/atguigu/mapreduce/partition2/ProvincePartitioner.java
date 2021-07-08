package com.atguigu.mapreduce.partition2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        int partition;
        if("136".equals(prePhone)){
            partition=0;
        }else if( "137".equals(prePhone)){
            partition=1;
        }else if("138".equals(prePhone)){
            partition=2;
        }else if("139".equals(prePhone)){
            partition=3;
        }else {
            partition=4;
        }

       /* switch (prePhone) {
            case "136":
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
            default:
                partition = 4;
        }*/
        return partition;
    }
}
