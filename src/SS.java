//SecondString
import com.wcohen.ss.*;
import com.wcohen.ss.api.*;
import com.wcohen.ss.tokens.*;

public class SS {
	public static void main(String args[]) {
		Tokenizer tokenizer = new AlphaNumericTokenizer(true, true);
		int n1 = Integer.parseInt(args[2]);
		int n2 = Integer.parseInt(args[3]);
		Tokenizer ngram = new NGramTokenizer(n1, n2, false, tokenizer);
		StringDistance dist = new Jaccard(ngram); 
	
		System.out.println(dist.explainScore(args[0], args[1]));
	}	
}

