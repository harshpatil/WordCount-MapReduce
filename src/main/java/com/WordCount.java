package com;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
    This is a Hadoop Map Reduce word count program.
    1) The map class implements Mapper interface and it processes one line at a time, as provided by the specified TextInputFormat.
       It then splits the line into tokens separated by whitespaces, via the StringTokenizer, applies filter on the word to
       remove whitespace characters and emits a key-value pair of the form : < <word>, 1>.

    2) The reduce class implements Reducer interface, it receives the input key as the word and the input value as a list of
       occurences of the word. It sums up the values in this list to calculate the total occurences of the word.

    3) The main driver class configues the hadoop job with all the configurations and starts it.

    Steps to run
    1) Make sure java 1.7 or above and hadoop is installed on the machine
    2) run : mvn clean install
    3) jar -> WordCount-MapReduce-1.0.0-1.0-SNAPSHOT.jar will get created under target folder. Copy this jar to EC2 machine
    4) start the hadoop server using : hadoop-2.7.3/sbin/start-dfs.sh
    5) Create input file directory in hdfs : hadoop-2.7.3/bin/hdfs dfs -mkdir -p /user/hpatil2/input
    6) copy the input text file to hdfs : hadoop-2.7.3/bin/hdfs dfs -put input.txt /user/hpatil2/input/
    7) Start program : hadoop-2.7.3/bin/hadoop jar /home/ec2-user/WordCount-MapReduce-1.0.0-1.0-SNAPSHOT.jar /user/hpatil2/input/input.txt /user/hpatil2/output
    8) Once program is executed, have a look at files under path : hadoop-2.7.3/bin/hdfs dfs -cat /user/hpatil2/output/*
 */


/**
 * Created by HarshPatil on 11/21/16.
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        System.out.println("Started executing");
        long startTime = System.currentTimeMillis();
        Configuration configuration = new Configuration();

        Job job = new Job(configuration, "wordcount");
        job.setNumReduceTasks(4);
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println("Time Taken in Milliseconds = " + (endTime-startTime));
        System.out.println("End of program");
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().replaceAll("[^a-zA-Z]", ""));
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum = sum + val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
