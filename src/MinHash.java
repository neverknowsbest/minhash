package com.nsrdev;

import java.util.*;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
* A simple MinHash implementation inspired by https://github.com/jmhodges/minhash
*
* @author tpeng (pengtaoo@gmail.com)
*/
public class MinHash {

    private HashFunction hash = Hashing.murmur3_32(0);
	private List<String> universe =  Arrays.asList("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890.,- ".split(""));
	private int numHashFunctions;
	private List<HashFunction> hashes;
	
	public MinHash(int nHashes) {
		numHashFunctions = nHashes;
		hashes = new ArrayList(nHashes);
		
		for (int i = 0;i < numHashFunctions;i++) {
			hashes.add(Hashing.murmur3_32(i));
		}
	}

    public String hash(String string) {
        int min = Integer.MAX_VALUE;
        String[] words = string.split(" ");

		for (String c : words) {
            int n = hash.newHasher().putString(c).hash().asInt();
			System.out.println(c + " " + Integer.toString(n));
            if (n < min) {
                min = n;
           	}
		}
        return Integer.toString(min);
    }

	public int[] hash2(String document) {
		int[] documentSlots = new int[numHashFunctions];
		int hashed;
		String rowString;
		HashFunction h;

		for (int i = 0;i < numHashFunctions;i++) {
			documentSlots[i] = Integer.MAX_VALUE;
		}
		
		for (int j = 0;j < universe.size();j++) {
			rowString = universe.get(j);
			if (document.indexOf(rowString) > 0) {
				for (int k = 0;k < numHashFunctions;k++) {
					h = hashes.get(k);
					hashed = h.newHasher().putInt(j).hash().asInt();
					
					if (hashed < documentSlots[k]) {
						documentSlots[k] = hashed;
					}
				}
			}
		}
		
		return documentSlots;
	}
	
	public double compare(String doc1, String doc2) {
		int[] sig1 = hash2(doc1);
		int[] sig2 = hash2(doc2);
		double similarity = 0;
		
		System.out.println(doc1);
		System.out.println(Arrays.toString(sig1));
		System.out.println(doc2);
		System.out.println(Arrays.toString(sig2));
				
		for (int i = 0;i < sig1.length;i++) {
			if (sig1[i] == sig2[i]) {
				similarity += 1.0;
			}
		}
		
		return similarity / sig1.length;
	}
	
	public double jaccard(String doc1, String doc2) {
		double score;
		Set<String> doc1Set = new HashSet<String>(Arrays.asList(doc1.split("")));
		Set<String> doc2Set = new HashSet<String>(Arrays.asList(doc2.split("")));
		
		System.out.println(Arrays.toString(doc1Set.toArray()));
		System.out.println(Arrays.toString(doc2Set.toArray()));
		
		score = (Sets.intersection(doc1Set, doc2Set).size() - 1.0)/(Sets.union(doc1Set, doc2Set).size() - 1.0);

		return score;
	}

    public static void main(String[] args) {
        MinHash minHash = new MinHash(Integer.parseInt(args[0]));

		System.out.println(minHash.compare(args[1], args[2]));
		System.out.println("Jaccard\n" + minHash.jaccard(args[1], args[2]));
    }
}