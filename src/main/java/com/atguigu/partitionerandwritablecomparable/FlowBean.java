package com.atguigu.partitionerandwritablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1 继承writable方法
 * 2 空参构造器
 * 3 重写write 和 read 方法
 * 4 重写tostring方法
 */
public class FlowBean implements WritableComparable<FlowBean>  {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    public FlowBean() {
    }


    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();

    }

    @Override
    public String toString() {
        return  upFlow+"\t" +downFlow +"\t"+ sumFlow ;
    }



    @Override
    public int compareTo(FlowBean o) {
        // 总流量倒序
        if(this.sumFlow > o.sumFlow){
            return -1;
        }else if(this.sumFlow < o.sumFlow){
            return 1;
        }else {
            // 上行流量正序
            if(this.upFlow > o.upFlow){
                return 1;
            }else if(this.upFlow < o.upFlow){
                return -1;
            }else {
                return 0;
            }
        }
    }
}

