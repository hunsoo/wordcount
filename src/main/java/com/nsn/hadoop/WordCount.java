package com.nsn.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    public WordCount() {
        System.out.println("Init WordCountTest");
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // strip off generic options and take command options, which are input and output file paths
        String[] commandArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (commandArgs.length != 2) {
            System.err.println("Usage: UFOLocationDriver <input path> <outputpath>");
            System.exit(-1);
        }

        Job job = new Job(conf, "Word Count");

        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapperClass(WordCountMapper.class);
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(commandArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(commandArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}