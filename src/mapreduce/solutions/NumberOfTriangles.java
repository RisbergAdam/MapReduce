package mapreduce.solutions;

import java.util.HashSet;
import java.util.Set;

import mapreduce.Emitter;
import mapreduce.Map;
import mapreduce.Reduce;

public class NumberOfTriangles implements Map<String, String>, Reduce<String, String> {

	public static void main(String [] args) {
		NumberOfTriangles nt = new NumberOfTriangles();
		SolutionUtils.MapReduce(nt, nt, SolutionUtils.parse("NumberOfTriangles", args));
	}
	
	@Override
	public void map(String fileName, String fileContent, Emitter<String, String> emitter) {
		String[] words = fileContent.split("\n");
		for (int i = 0; i < words.length; i++) {
			words[i] = words[i].replace("(", "").replace(")", "");
		}
		for (String word : words) {
			String[] tmp = word.split(",");
			int nr = Integer.parseInt(tmp[0]);

			for (int i = 0; i <= nr; i++) {
				emitter.emit(i + "", word);
			}
		}
	}

	@Override
	public String reduce(String key, String[] values) {
		int end = values.length;
		Set<String> set = new HashSet<String>();
		Set<String> set2 = new HashSet<String>();

		for (int i = 0; i < end; i++) {
			String[] vals = values[i].split(",");
			if (vals[0].equals(key))
				set2.add(vals[1]);
			if (vals[1].equals(key))
				set2.add(vals[0]);
			set.add(values[i]);
		}
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(key);
		int count = 0;
		for (String friend : set) {
			String[] parts = friend.split(",");
			if (set2.contains(parts[0]) && set2.contains(parts[1])) {
				count++;

			}

		}
		stringBuilder.append(" " + count);
		return stringBuilder.toString();

	}

}