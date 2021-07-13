package com.atguigu.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> order = new ArrayList<>();
        TableBean pd = new TableBean();
        // 对取自不同的表的values【TableBean】,
            //1  区分取自不同的表的value
        for (TableBean value : values) {
            System.out.println("reducer/reducer/reducer/reducer/reducer/reducer/");
            System.out.println(key);
            System.out.println(value);
            System.out.println("***************************************");
            if("order".equals(value.getFlag())){
                System.out.println("走的order表："+value);

                TableBean tmpBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpBean,value);

                    System.out.println("order表的："+ tmpBean);

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                order.add(tmpBean);
                System.out.println("array集合的情况："+order);
                System.out.println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");


            } else {
                System.out.println("走的pd表："+ value);
                try {
                    BeanUtils.copyProperties(pd,value);

                    System.out.println("pd对象的情况："+pd);
                    System.out.println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

        }
        // 循环遍历order
        for (TableBean orderBean : order) {

            orderBean.setPname(pd.getPname());
            System.out.println("循环写出之前KEY："+key);
            System.out.println("循环写出之前VALUE："+orderBean);

            context.write(orderBean,NullWritable.get());
        }

    }
}
