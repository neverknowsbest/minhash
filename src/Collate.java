package com.nsrdev;

import com.nsrdev.util.*;

import com.google.common.hash.*;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//SecondString
import com.wcohen.ss.*;
import com.wcohen.ss.api.*;
import com.wcohen.ss.tokens.*;

public class Collate extends Configured implements Tool {	
	static enum Counters {COMPARISONS, REDUCES}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {			
		/**
		Get Hadoop input parameters from configuration.
		
		@param	job	the Hadoop JobConf 
		*/
		public void configure(JobConf job) {

		}
				
		/**
		Hadoop map function that performs MinHashing of input records. 
		
		@param	key		Hadoop key (not used)
		@param	value	Hadoop value. Text object containing one line of input from the input file.
		@param	output	Hadoop output collector. Output format is (MinHash(value), value), where value is the input value.
		@param	reporter	Hadoop reporter.
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String match = value.toString().split("\t")[1];
			String[] matches = match.substring(1, match.indexOf(")")).split(",");
			if (Long.parseLong(matches[0]) < Long.parseLong(matches[1])) {
				output.collect(new Text(matches[0]), new Text(matches[1]));
			} else {
				output.collect(new Text(matches[1]), new Text(matches[0]));
			}
			
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Long, Text> {
		/**
		Get Hadoop input parameters from configuration.
		
		@param	job	the Hadoop JobConf 
		*/
		public void configure(JobConf job) {
		}
		
		/**		
		@param	key		Text representation of the MinHash of an input line
		@param	values	List of strings that have the MinHash value in 'key'
		
		*/
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Long, Text> output, Reporter reporter) throws IOException {
			String combined = new String();
			
			while (values.hasNext()) {
				Text value = values.next();
				if (combined.isEmpty()) {
					combined = value.toString();
				} else {
					combined = combined + "," + value.toString();
				}
				
			}
			output.collect(Long.parseLong(key.toString()), new Text(combined));
		}
	}	
	
	/**
	Create Hadoop job configuration, including input parameters.
	
	@param	numHashes	the number of hash functions to use when calculating the MinHash
	@param	similarityFunction	the SecondString similarity function to use
	@param	threshold	the threshold value for the similarity function
	*/
	private JobConf createJobConf() {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, Collate.class);
		
		job.setJobName("collate");
		
		//set map output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//set reduce output types
		job.setOutputKeyClass(Long.class);
		job.setOutputValueClass(Text.class);
		
		//set Map, Combine, and Reduce classes
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//set overall input and output formats
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		return job;
	}

    public int run(String[] args) throws Exception {
		JobConf job = createJobConf();

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		
		return 0;
    }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Collate(), args);

	}
}