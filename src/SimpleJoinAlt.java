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

//SoftTFIDF
import com.wcohen.ss.*;
import com.wcohen.ss.api.*;
import com.wcohen.ss.tokens.*;

public class SimpleJoinAlt extends Configured implements Tool {
	private static double threshold = 0.9;
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			//SoftTFIDF
			Tokenizer tokenizer = new SimpleTokenizer(true, true);
			SoftTFIDF dist = new SoftTFIDF(tokenizer, new JaroWinkler(), threshold);

			String[] splitInput = value.toString().split("\n");
			String lineA, lineB, idA, addrA, idB, addrB;
			String[] splitLine;
			String scoreString;
			double score;
						
			for(String line:splitInput) {
				splitLine = line.split(":");
				lineA = splitLine[0];
				lineB = splitLine[1];
		
				idA = lineA.split(",")[0];
				addrA = lineA.split(",")[1];
				idB = lineB.split(",")[0];
				addrB = lineB.split(",")[1];
	
				scoreString = dist.explainScore(addrA, addrB);

				score = Double.parseDouble(scoreString.split("\n")[1].split("=")[1]);

				if (score > threshold) {
					output.collect(key, new Text("(" + idA + "," + idB + "," + addrA + "," + addrB + "," + scoreString + ")"));
				}
			}
		}
    }

    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
      public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
        String sum = "";
        while (values.hasNext()) {
          sum += values.next().toString();
			sum += " ";
        }
        output.collect(key, new Text(sum));
      }
    }

    public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SimpleJoin.class);
		
		job.setJobName("simplejoin");

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
		int res = ToolRunner.run(new Configuration(), new SimpleJoinAlt(), args);

	}
}