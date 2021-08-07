package com.atguigu.videoETL2;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Liang on 2020/3/11.
 * <p>
 * 清洗数据 集合类型清洗为同种间隔符号
 * 去掉脏数据
 * 去掉类别中的空格
 * 相关视频空格用&代替
 */

public class ETLMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.读数据
        String s = value.toString();
        //2 清洗数据
        String afterData = etldata(s);
        if (StringUtils.isEmpty(afterData)) {
            return;
        }
        // 包装数据 写数据
        k.set(afterData);
        context.write(k, NullWritable.get());
    }

    /*数据清洗的逻辑*/
    public String etldata(String s) {
        String[] split = s.split("\t");
        int strLength = split.length;
        if (strLength < 9) {
            return "";
        }
        split[3] = split[3].replaceAll(" ", "");
        StringBuilder str = new StringBuilder();
        // 返回数据
        for (int i = 0; i < strLength; i++) {
            // 长度等于9 和10
            if (strLength == 9 || strLength == 10) {
                if (i == strLength - 1) {
                    str.append(split[i]);
                } else {
                    str.append(split[i] + "\t");

                }
            } else {
                if (i < 9) {
                    str.append(split[i] + "\t");
                } else {
                    if (i == strLength - 1) {
                        str.append(split[i]);
                    } else {
                        str.append(split[i] + "&");
                    }
                }


            }


        }


        return str.toString();

    }


}
