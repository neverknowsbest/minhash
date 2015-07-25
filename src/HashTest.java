import com.google.common.hash.*;

import java.io.*;
import java.util.*;

public class HashTest {
	private static int numHashes;
	private static List<HashFunction> hashes;
	
	public static void main(String args[]) {
		BufferedReader br;
		hashes = new ArrayList(numHashes);
		
		try {
			 br = new BufferedReader(new FileReader("../../input/test_input.csv"));

		} catch (FileNotFoundException e) {
			return;
		}
		
		try {
			String line = br.readLine();
			String hash;
			numHashes = 3;

			for (int i = 0;i < numHashes;i++) {
				hashes.add(Hashing.murmur3_32(i));
			}
		
			while (line != null) {
				hash = hash(line.split(",")[1]);
				System.out.println(line + " " + hash);
				line = br.readLine();
			}			
		} catch (IOException e) {
			return;
		}
		
		return;
	}
	
	private static String hash(String document) {
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

				// System.out.println(c);
				if (hashed < documentSlots[k]) {
					documentSlots[k] = hashed;
				}					
			}
		}

		return Arrays.toString(documentSlots);
	}	

}