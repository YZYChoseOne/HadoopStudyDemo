package com.atguigu.mapreduce.partition2;

public class Text {
    public static void main(String[] args) {
        String str = "1367658";
        String sub1 = str.substring(0, 3);
        System.out.println(sub1);
        if(sub1=="136"){
            System.out.println("good");
        }else System.out.println("bad");
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
