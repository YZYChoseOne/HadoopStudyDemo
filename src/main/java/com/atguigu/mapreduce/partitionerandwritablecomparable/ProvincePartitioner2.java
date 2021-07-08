package com.atguigu.mapreduce.partitionerandwritablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        int partition;
        String phone = text.toString();
        String subPhone = phone.substring(0, 3);
        if("136".equals(subPhone)){
            partition = 0;
        }else if("137".equals(subPhone)){
            partition = 1;
        }else if("138".equals(subPhone)){
            partition = 2;
        }else if("139".equals(subPhone)){
            partition = 3;
        }else {
            partition = 4;
        }
        return partition;
    }

}
