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

public class MinHashMR extends Configured implements Tool {	
	static enum Counters {COMPARISONS, REDUCES}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private static int numHashes;
		private List<HashFunction> hashes;
			
		/**
		Get Hadoop input parameters from configuration.
		
		@param	job	the Hadoop JobConf 
		*/
		public void configure(JobConf job) {
			numHashes = Integer.parseInt(job.get("NumHash"));
			hashes = new ArrayList(numHashes);
			
			for (int i = 0;i < numHashes;i++) {
				hashes.add(Hashing.murmur3_32(i));
			}
		}
		
		/**
		Calculates the MinHash of a String. The MinHash of a string S is the minimum value of numHashes hash functions h1(),...,hnumHashes(), over each n-gram in S. In this case, n=1, which means each hash function is evaluated on each letter in the input.
		
		@param	document	the document to calculate the MinHash over
		@return 			the MinHash, as a string representation of a numHashes-tuple
		*/
		private String hash(String document) {
			int[] documentSlots = new int[numHashes];
			int hashed;
			String rowString;
			HashFunction h;

			for (int i = 0;i < numHashes;i++) {
				documentSlots[i] = Integer.MAX_VALUE;
			}
			
			for (int j = 0;j < document.length();j++) {
				for (int k = 0;k < numHashes;k++) {
					h = hashes.get(k);
					char c = document.charAt(j);
					hashed = h.newHasher().putChar(c).hash().asInt();
					
					if (hashed < documentSlots[k]) {
						documentSlots[k] = hashed;
					}					
				}
			}

			return Arrays.toString(documentSlots);
		}
		
		/**
		Hadoop map function that performs MinHashing of input records. 
		
		@param	key		Hadoop key (not used)
		@param	value	Hadoop value. Text object containing one line of input from the input file.
		@param	output	Hadoop output collector. Output format is (MinHash(value), value), where value is the input value.
		@param	reporter	Hadoop reporter.
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String stringHash = hash(value.toString().split(",")[1]);
			output.collect(new Text(stringHash), value);
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Long, Text> {
		private String similarityFunction;
		private double threshold;

		/**
		Get Hadoop input parameters from configuration.
		
		@param	job	the Hadoop JobConf 
		*/
		public void configure(JobConf job) {
			similarityFunction = job.get("SimilarityFunction");
			threshold = Double.parseDouble(job.get("Threshold"));
		}
		
		/**
		MinHash reduce function. Keys are aggregated by MinHash value, then a nested loop self-join is performed on the aggregate bucket. Output collects records with similarity scores over the threshold.
		
		@param	key		Text representation of the MinHash of an input line
		@param	values	List of strings that have the MinHash value in 'key'
		
		*/
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Long, Text> output, Reporter reporter) throws IOException {
			Text textValue;
			List<String> hashBucketValues = new ArrayList();
			String recordA, recordB, idA, idB, scoreString, value;
			double score;
			Set<String> matched = new HashSet<String>();
			
			SimpleJoinArgs inputArgs = new SimpleJoinArgs("", "", similarityFunction, threshold);
			StringDistance dist = inputArgs.getStringDistance();
			
			while (values.hasNext()) {
				value = values.next().toString();
				hashBucketValues.add(value);
				// output.collect(Long.valueOf(hashBucketValues.size()), key);
			}
			
			System.err.println("Length of reduce set:" + Integer.toString(hashBucketValues.size()));
			
			if (hashBucketValues.size() > 1) {
				reporter.incrCounter(Counters.REDUCES, hashBucketValues.size());

				for (int i = 0;i < hashBucketValues.size();i++) {
					idA = hashBucketValues.get(i).split(",")[0];
					recordA = hashBucketValues.get(i).split(",")[1];
					for (int j = 0;j < hashBucketValues.size();j++) {
						idB = hashBucketValues.get(j).split(",")[0];
						recordB = hashBucketValues.get(j).split(",")[1];
					
						if (i == j || (matched.contains(idA) && matched.contains(idB)))
							continue;
												
						score = dist.score(recordA, recordB);
						reporter.incrCounter(Counters.COMPARISONS, 1);
						
						// output.collect(1L, new Text(idA + " " + idB + " " + score));
						
						if (score > inputArgs.getThreshold()) {
							if (Long.parseLong(idA) < Long.parseLong(idB)) {
								output.collect(Long.parseLong(idA), new Text("(" + idA + "," + idB + "," + recordA + "," + recordB + ")"));
							} else {
								output.collect(Long.parseLong(idB), new Text("(" + idB + "," + idA + "," + recordB + "," + recordA + ")"));
							}
							
							
							matched.add(idA);
							matched.add(idB);
						}	
					}
					reporter.progress();
				}
			}
		}
	}	
	
	/**
	Create Hadoop job configuration, including input parameters.
	
	@param	numHashes	the number of hash functions to use when calculating the MinHash
	@param	similarityFunction	the SecondString similarity function to use
	@param	threshold	the threshold value for the similarity function
	*/
	private JobConf createJobConf(String numHashes, String similarityFunction, String threshold) {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, MinHash.class);
		
		job.setJobName("minhash");
		
		job.set("NumHash", numHashes);
		job.set("SimilarityFunction", similarityFunction);
		job.set("Threshold", threshold);
		
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
		JobConf job = createJobConf(args[2], args[3], args[4]);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		
		return 0;
    }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MinHashMR(), args);

	}
}