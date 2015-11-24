package mapreduce.solutions;

import java.util.HashSet;
import java.util.Set;

import mapreduce.Emitter;
import mapreduce.Map;
import mapreduce.Reduce;

public class GraphConversion implements Reduce<String, String>, Map<String, String> {

	public static void main(String [] args) {
		GraphConversion gc = new GraphConversion();
		SolutionUtils.MapReduce(gc, gc, SolutionUtils.parse("GraphConversion", args));
	}
	
	@Override
	public void map(String fileName, String fileContent, Emitter<String, String> emitter) {
		String[] words = fileContent.split("\n");
		for (String word : words) {
			word = word.replace("(", "").replace(")", "");
			String[] tmp = word.split(",");
			emitter.emit(tmp[0], tmp[1]);
			emitter.emit(tmp[1], tmp[0]);
		}

	}
	
	@Override
	public String reduce(String key, String[] values) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(key + " # ");
		Set<String> map = new HashSet<String>();
		for (String val : values) {
			if (map.add(val))
				stringBuilder.append(val + " ");
		}
		return stringBuilder.toString();
	}

}
