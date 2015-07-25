package com.nsrdev;

import com.nsrdev.util.*;

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
import com.wcohen.ss.api.*;

public class SimpleJoin extends Configured implements Tool {	
	static enum Counters {COMPARISONS}
	
	public static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable() {
			super(Text.class);
		}
		
		public TextArrayWritable(Writable[] values) {
			super(Text.class, values);
		}
	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		private static String similarityFunction;
		private static double threshold;
		
		public void configure(JobConf job) {
			similarityFunction = job.get("SimilarityFunction");
			threshold = Double.parseDouble(job.get("Threshold"));
		}		
		
		/**
		 * Hadoop map function that performs a simple nested loop join over the files named in the input. The input should be formatted as a list of files, with two filenames per line, separated by a space.
		 *
		 *
		 */
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			String filenames = value.toString();
			String[] splitFilenames = filenames.split(" ");
			System.err.println(filenames);
			Set<String> matched = new HashSet<String>();
			SimpleJoinArgs inputArgs = new SimpleJoinArgs("", "", similarityFunction, threshold);
			
			//Get filenames from input value, open files for comparison
			Configuration conf = new Configuration();
			Path a = new Path(splitFilenames[0]);
			Path b = new Path(splitFilenames[1]);
			FileSystem fs = FileSystem.get(a.toUri(), conf);
			FSDataInputStream inA = fs.open(a);
			FSDataInputStream inB = fs.open(b);
			BufferedReader brA = new BufferedReader(new InputStreamReader(inA));
			BufferedReader brB = new BufferedReader(new InputStreamReader(inB));
			
			// TextArrayWritable outputAW;
			// Set<Text> outputSet;
			String[] splitLineA, splitLineB;
			String lineA, lineB, idA, addrA, idB, addrB;
			String scoreString;
			double score;
			
			StringDistance dist = inputArgs.getStringDistance();
			addrA = "";
			
			while ((lineA = brA.readLine()) != null) {
				// outputSet = new HashSet<Text>();
				splitLineA = lineA.split(",");
				if (splitLineA[0].split("\t").length > 0) {
					idA = splitLineA[0].split("\t")[1];
				} else {
					idA = splitLineA[0];
				}
				
				try {
					addrA = splitLineA[1];
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out.println(lineA);
					System.err.println(lineA);
					throw e;
				}

				while ((lineB = brB.readLine()) != null) {
					splitLineB = lineB.split(",");
					// idB = splitLineB[0].split("\t")[1];
					if (splitLineB[0].split("\t").length > 0) {
						idB = splitLineB[0].split("\t")[1];
					} else {
						idB = splitLineB[0];					
					}					
					addrB = splitLineB[1];
			
					if (idA.equals(idB) || (matched.contains(idA) && matched.contains(idB))) 
						continue;

					// scoreString = dist.explainScore(addrA, addrB);
					// scoreString = "";
					score = dist.score(addrA, addrB);
					reporter.incrCounter(Counters.COMPARISONS, 1);

					if (score > inputArgs.getThreshold()) {
						output.collect(new LongWritable(Long.parseLong(idA)), new Text("(" + idA + "," + idB + "," + addrA + "," + addrB + ")"));
						// outputSet.add(new Text("(" + idA + "," + idB + "," + addrA + "," + addrB + "," + scoreString + ")"));
						matched.add(idA);
						matched.add(idB);
					}
				}
				// outputAW = new TextArrayWritable(outputSet.toArray(new Text[outputSet.size()]));
				// output.collect(key, outputAW);
				inB.seek(0);
			}
		}
	}
	
	public static class Combine extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			String combinedString = new String();
			Text textValue;
			while (values.hasNext()) {
				textValue = values.next();
				
				combinedString += textValue.toString() + "%";
			}
			output.collect(key, new Text(combinedString));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			// TextArrayWritable textAW;
			// Text[] textArray;
			Text textValue;
			String[] stringValues;
			while (values.hasNext()) {
				// textAW = values.next();
				textValue = values.next();
				stringValues = textValue.toString().split("%");
				for (String result:stringValues) {
					if (result.length() > 0)
						output.collect(key, new Text(result));
				}
					
				// textArray = (Text[]) textAW.toArray();
				// for (Text result:textArray)
				// 	output.collect(key, result);
			}
		}
	}

	private Path writeToTempFilePath(String buffer, FileSystem fs) throws IOException {
		Path outFile = new Path("hdfs:///temp.txt");
		FSDataOutputStream out = fs.create(outFile);
		
		out.writeBytes(buffer);
		out.close();
		
		return outFile;
	}

	/**
	* Process input directory - clean out empty/invalid files, 
	* create a temp. file that contains all possible combinations 
	* of the valid files, on separate lines, separated by a space: 
	* input-00000 input-00000
	* input-00000 input-00001
	* input-00001 input-00000
	* input-00001 input-00001
	*
	*
	*/
	private Path createInputFilePath(Path inputPath, FileSystem fs) throws IOException {
		FileStatus files[] = fs.listStatus(inputPath);
		String mapInput = new String();
		String hadoopPathString = "hdfs:///";
		
		for (FileStatus file:files) {
			for (FileStatus file2:files) {
				if (file.getPath().getName().equals("_SUCCESS") || file2.getPath().getName().equals("_SUCCESS"))
					continue;

				mapInput +=  hadoopPathString + inputPath.getName() + "/" + file.getPath().getName() + " " + hadoopPathString + inputPath.getName() + "/" + file2.getPath().getName() + "\n";
			}
		}

		return writeToTempFilePath(mapInput, fs);
	}

	/**
	 * Process command-line arguments.
	 *
	 * @param	args		array of command line arguments from the main method
	 * @return	simpleArgs	SimpleJoinArgs object containing the arguments
	 */
	private SimpleJoinArgs processArguments(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(inputPath.toUri(), getConf());		
		String inputString;
		SimpleJoinArgs simpleArgs;
		
		//If input path is a directory, treat it as output from a Hadoop program, ie. expect it to be split into multiple, mutually exclusive files
		if(fs.isDirectory(inputPath)) {
			inputPath = createInputFilePath(inputPath, fs);
		}

		simpleArgs = new SimpleJoinArgs(inputPath, outputPath, args[2], Double.parseDouble(args[3]));
		return simpleArgs;
	}	
	
	private JobConf createJobConf(SimpleJoinArgs inputArgs) {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SimpleJoin.class);
		
		job.setJobName("simplejoin");
		
		//pass mapper arguments through job conf
		job.set("SimilarityFunction", inputArgs.getSimilarityFunction());
		job.set("Threshold", Double.toString(inputArgs.getThreshold()));
		
		//set map output types
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//set reduce output types
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		//set Map, Combine, and Reduce classes
		job.setMapperClass(Map.class);
		// job.setNumReduceTasks(0);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		//set overall input and output formats
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		return job;
	}

    public int run(String[] args) throws Exception {
		SimpleJoinArgs inputArgs = processArguments(args);
		JobConf job = createJobConf(inputArgs);

		FileInputFormat.setInputPaths(job, inputArgs.getInputPath());
		FileOutputFormat.setOutputPath(job, inputArgs.getOutputPath());
		
		JobClient.runJob(job);
		
		return 0;
    }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SimpleJoin(), args);

	}
}