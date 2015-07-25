package com.nsrdev;

import com.nsrdev.util.*;

import java.io.*;
import java.util.*;

//SecondString
import com.wcohen.ss.api.*;

public class SimpleJoinSingle {
	private static Set<String> matches = new HashSet<String>();
	private static boolean VERBOSE = false;
	
	/**
	 * Perform the nested loop join on all files in the input.
	 *
	 * @param	inputArgs	a SimpleJoinArgs object containing the input arguments
	 */
	public static void joinAll(SimpleJoinArgs inputArgs) throws Exception {
		String[] inputStrings = inputArgs.getInputString().split("\n");
		for (String line:inputStrings)
			join(line, inputArgs);
	}
	
	/**
	 * A simple nested loop join. Compares every item in the input set to itself, for a total of N^2 comparisons.
	 * 
	 * @param	inputLine	a string describing the input files to perform the join over
	 * @param	inputArgs	a SimpleJoinArgs object containing the input arguments
	 */
	private static void join(String inputLine, SimpleJoinArgs inputArgs) throws FileNotFoundException, IOException {
		String[] splitFilenames = inputLine.split(" ");
		Set<String> matched = new HashSet<String>();
		double count = 0;
		double innerCount = 0;
		
		//Get filenames from input value, open files for comparison
		FileInputStream inA = new FileInputStream(splitFilenames[0]);
		BufferedReader brA = new BufferedReader(new InputStreamReader(inA));
		
		StringDistance dist = inputArgs.getStringDistance();
		
		String[] splitLineA, splitLineB;
		String lineA, lineB, idA, addrA, idB, addrB;
		String scoreString;
		double score;

		//Nested loop record linkage
		while ((lineA = brA.readLine()) != null) {
			//Split input lines from file "A" into (id, value) pairs
			splitLineA = lineA.split(",");
			if (splitLineA[0].split("\t").length > 1) {
				idA = splitLineA[0].split("\t")[1];
			} else {
				idA = splitLineA[0];
			}
			if (splitLineA.length > 1) {
				addrA = splitLineA[1];
			} else {
				System.out.println(lineA);
				continue;
			}
			
			FileInputStream inB = new FileInputStream(splitFilenames[1]);
			BufferedReader brB = new BufferedReader(new InputStreamReader(inB));
			innerCount = 0;
			
			while ((lineB = brB.readLine()) != null) {
				//Split input lines from file "B"
				innerCount += 1;
				splitLineB = lineB.split(",");
				if (splitLineB[0].split("\t").length > 1) {
					idB = splitLineB[0].split("\t")[1];
				} else {
					idB = splitLineB[0];
				}
				if (splitLineB.length > 1) {
					addrB = splitLineB[1];
				} else {
					// System.out.println(lineB);
					continue;
				}
			
				if (idA.equals(idB) || (matched.contains(idA) && matched.contains(idB))) 
					continue;

				// scoreString = dist.explainScore(addrA, addrB);
				// score = Double.parseDouble(scoreString.split("\n")[scoreString.split("\n").length-1].split("=")[1]);
				score = dist.score(addrA, addrB);
				
				if(VERBOSE && addrA.contains("630227") && addrB.contains("630227")) {
					System.out.println(addrA);
					System.out.println(addrB);
					System.out.println(score);
					scoreString = dist.explainScore(addrA, addrB);
					System.out.println(scoreString);
				}
				
				if (score > inputArgs.getThreshold()) {
					matches.add("(" + idA + "," + idB + "," + addrA + "," + addrB + ")");
					matched.add(idA);
					matched.add(idB);
				}
			}
			inB.close();
			System.out.println(count/innerCount); //output percentage done
			count += 1;
		}
	}
	
	/**
	 * Write all output to the file specified in the filename.
	 *
	 * @param	filename	the output filename
	 */
	private static void writeOutput(String filename) throws IOException {
		FileWriter fstream = new FileWriter(filename);
		BufferedWriter out = new BufferedWriter(fstream);
		Iterator<String> itr = matches.iterator();
		
		while(itr.hasNext()) {
			String outputLine = itr.next();
			out.write(outputLine, 0, outputLine.length());
			out.newLine();
		}
		out.close();
	}
	
	/**
	 * Create a temporary input file from a directory of input files.
	 *
	 * @param	inputDir	the input directory
	 * @returns	inputString	a String of lines with file names separated by a space
	 */
	private static String createInputString(File inputDir) throws IOException {
		File[] directoryFiles = inputDir.listFiles();
		String inputString = new String();
		
		for(File file:directoryFiles) {
			for(File file2:directoryFiles) {
				if((file.getName().equals("_SUCCESS")) || (file2.getName().equals("_SUCCESS"))) 
					continue;

				inputString += file.getPath() + " " + file2.getPath() + "\n";
			}
		}
		
		return inputString;
	}
	
	/**
	 * Process command-line arguments.
	 *
	 * @param	args		array of command line arguments from the main method
	 * @returns	simpleArgs	SimpleJoinArgs object containing the arguments
	 */
	private static SimpleJoinArgs processArguments(String[] args) throws Exception {
		if (args.length > 4) {
			VERBOSE = true;
		} 
		File inputFile = new File(args[0]);
		String inputString;
		SimpleJoinArgs simpleArgs;
		
		if(inputFile.isDirectory()) {
			inputString = createInputString(inputFile);
		} else {
			inputString = args[0] + " " + args[0];
		}
		
		simpleArgs = new SimpleJoinArgs(inputString, args[1], args[2], Double.parseDouble(args[3]));
		return simpleArgs;
	}
	
	public static void main(String args[]) throws IOException, Exception {
		SimpleJoinArgs inputArgs = processArguments(args);
		
		long startTime = System.nanoTime();
		joinAll(inputArgs);
		long endTime = System.nanoTime();
		
		double duration = (endTime - startTime)/1000000000.;
		
		writeOutput(inputArgs.getOutputString());
		System.out.println(duration);
	}
}