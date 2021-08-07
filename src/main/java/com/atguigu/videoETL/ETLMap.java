package com.atguigu.videoETL;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * testGitPush
 * Created by Liang on 2020/3/1.
 * <p>
 * 清洗数据
 */

public class ETLMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1.读取数据
        String splitLine = value.toString();
        //2 .清洗数据
        String etlStr = ETLUtil(splitLine);
        //3.写数据
        if (StringUtils.isBlank(etlStr)) {
            return;
        }
        k.set(etlStr);
        context.write(k, NullWritable.get());
    }


    /**
     * @param
     * @return 清洗掉小于9的脏数据
     * 将类别中空格替换掉
     * 将相关视频的分隔符替换为&
     */
    public String ETLUtil(String splitLines) {
        String[] splitLine = splitLines.split("\t");
        //清洗掉小于9的脏数据
        if (splitLine.length < 9) {
            return null;
        }
        //将类别中空格替换掉
        splitLine[3] = splitLine[3].replaceAll(" ", "");
        // 将相关视频的分隔符替换为&
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < splitLine.length; i++) {
            if (splitLine.length == 9) {
                if (splitLine.length - 1 == i) {
                    str.append(splitLine[i]);
                } else {
                    str.append(splitLine[i] + "\t");
                }
            } else {
                // 默认length>9
                if (i < 9) {
                    str.append(splitLine[i] + "\t");
                } else {
                    if (splitLine.length - 1 == i) {
                        str.append(splitLine[i]);
                    } else {

                        str.append(splitLine[i] + "&");
                    }

                }


            }
        }
        return str.toString();
    }


}
