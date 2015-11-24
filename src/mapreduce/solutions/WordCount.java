package mapreduce.solutions;

import java.util.Scanner;

import mapreduce.Emitter;
import mapreduce.Map;
import mapreduce.Reduce;

public class WordCount implements Reduce<String, Integer>, Map<String, Integer> {
	
	public static void main(String [] args) {
		WordCount wc = new WordCount();
		SolutionUtils.MapReduce(wc, wc, SolutionUtils.parse("WordCount", args));
	}

	@Override
	public void map(String fileName, String fileContent, Emitter<String, Integer> emitter) {
    	Scanner sc = new Scanner(fileContent);
    	while (sc.hasNext()) emitter.emit(sc.next().toLowerCase().replace("\n", ""), 1);
    	sc.close();
	}

	@Override
	public String reduce(String key, Integer[] values) {
		return key + " " + values.length;
	}

}
