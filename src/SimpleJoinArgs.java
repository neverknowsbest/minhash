package com.nsrdev.util;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;

//SecondString
import com.wcohen.ss.*;
import com.wcohen.ss.api.*;
import com.wcohen.ss.tokens.*;

public class SimpleJoinArgs {
	private String inputString = "";
	private String outputString = "";
	private Path inputPath;
	private Path outputPath;
	private String similarityFunction;
	private double threshold;
	
	public SimpleJoinArgs(String argInput, String argOutput, String argSimilarityFunction, double argThreshold) {
		inputString = argInput;
		outputString = argOutput;
		similarityFunction = argSimilarityFunction;
		threshold = argThreshold;
	}
	
	public SimpleJoinArgs(Path argInput, Path argOutput, String argSimilarityFunction, double argThreshold) {
		inputPath = argInput;
		outputPath = argOutput;
		similarityFunction = argSimilarityFunction;
		threshold = argThreshold;
	}
	
	public String getInputString() {
		return inputString;
	}
	
	public String getOutputString() {
		return outputString;
	}
	
	public Path getInputPath() {
		return inputPath;
	}
	
	public Path getOutputPath() {
		return outputPath;
	}
	
	public String getSimilarityFunction() {
		return similarityFunction;
	}
	
	public double getThreshold() {
		return threshold;
	}
	
	public StringDistance getStringDistance() {
		//Select string similarity function
		Tokenizer tokenizer = new AlphaNumericTokenizer(true, true);
		Tokenizer ngram = new NGramTokenizer(2, 2, false, tokenizer);
		StringDistance dist = new Jaccard(ngram); //Jaccard is default		
		if (similarityFunction.equals("SoftTFIDF")) {
			dist = new SoftTFIDF(ngram, new JaroWinkler(), threshold);
		} else if (similarityFunction.equals("Jaccard")) {
			dist = new Jaccard(ngram);
		} else if (similarityFunction.equals("Levenstein")) {
			dist = new Levenstein();
		} else if (similarityFunction.equals("Level2JaroWinkler")) {
			dist = new Level2JaroWinkler();	
		} 
		return dist;
	}
}