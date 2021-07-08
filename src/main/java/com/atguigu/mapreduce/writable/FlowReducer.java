package com.atguigu.mapreduce.writable;

import org.apache.avro.io.parsing.Symbol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean outValue = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Long totalUp = 0L;
        Long totalDown = 0L;

        for (FlowBean bean : values) {
            totalUp += bean.getUpFlow();
            totalDown += bean.getDownFlow();
        }

        outValue.setUpFlow(totalUp);
        outValue.setDownFlow(totalDown);
        outValue.setSumFlow();

        context.write(key,outValue);

    }
}
