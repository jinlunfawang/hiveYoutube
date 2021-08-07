package com.atguigu.videoETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Liang on 2020/3/1.
 */

public class ELTDriver {

    public static void main(String[] args) throws Exception {
        Job job= Job.getInstance(new Configuration());
        //--------map--------
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setJarByClass(ELTDriver.class);
        job.setMapperClass(ETLMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //---------reduce----------
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);




    }
}
