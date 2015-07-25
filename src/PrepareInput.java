package com.nsrdev;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PrepareInput extends Configured implements Tool {	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			output.collect(key, value);
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, PrepareInput.class);
		
		job.setJobName("prepareinput");

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		// job.setCombinerClass(Reduce.class);
		// job.setReducerClass(Reduce.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		JobClient.runJob(job);
		
		return 0;
    }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PrepareInput(), args);
	}
}