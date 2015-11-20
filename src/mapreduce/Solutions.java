package mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class Solutions {

	public static void main(String[] arg) {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Enter input directory: ");

		String input = scanner.next();

		System.out.print("Enter output directory: ");

		String output = scanner.next();

		System.out.print("Enter number of map threads: ");

		int mapThreads = scanner.nextInt();

		System.out.print("Enter number of reduce threads: ");

		int reduceThreads = scanner.nextInt();

		System.out.print(
				"What program to run (1-4)?  \n1. WordCount\n2. GraphConversion\n3. CommonFriends\n4. TriangleFriends\n");

		int program = scanner.nextInt();

		MapReduce<String, String> master = new MapReduce<>(mapThreads, reduceThreads);

		switch (program) {
		case 1:
			WordCount wordCount = new WordCount();
			MapReduce<String, Integer> master2 = new MapReduce<>(mapThreads, reduceThreads);
			master2.applyMap(wordCount, input);
			master2.applyReduce(wordCount, output);
			master2.killThreads();
			break;
		case 2:
			GraphConversion graphConversion = new GraphConversion();
			master.applyMap(graphConversion, input);
			master.applyReduce(graphConversion, output);
			master.killThreads();
			break;
		case 3:
			CommonFriends commonFriends = new CommonFriends();
			master.applyMap(commonFriends, input);
			master.applyReduce(commonFriends, output);
			master.killThreads();
			break;
		case 4:
			NumberOfTriangles numberOfTriangles = new NumberOfTriangles();
			master.applyMap(numberOfTriangles, input);
			master.applyReduce(numberOfTriangles, output);
			master.killThreads();
			break;
		default:
			break;
		}
		master.killThreads();
	}

}

class WordCount implements Reduce<String, Integer>, Map<String, Integer> {

	@Override
	public void map(String fileName, String fileContent, Emitter<String, Integer> emitter) {
		String[] lines = fileContent.split("\n");
		for (String line : lines) {
			String[] words = line.split(" ");
			for (String word : words) {
				emitter.emit(word, 1);
			}
		}

	}

	@Override
	public String reduce(String key, Integer[] values) {
		return key + " " + values.length;
	}

}

class GraphConversion implements Reduce<String, String>, Map<String, String> {

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

}

class CommonFriends implements Map<String, String>, Reduce<String, String> {

	@Override
	public void map(String fileName, String fileContent, Emitter<String, String> emitter) {
		String[] words = fileContent.split("\n");
		for (String word : words) {
			String word2 = word.replace("(", "").replace(")", "");
			String[] tmp = word2.split(",");
			String ret = tmp[0] + " " + tmp[1];
			if (Integer.parseInt(tmp[0]) > Integer.parseInt(tmp[1]))
				ret = tmp[1] + " " + tmp[0];
			for (String friend : words) {
				String friend2 = friend.replace("(", "").replace(")", "");
				String[] fTmp = friend2.split(",");
				if ((fTmp[0].equals(tmp[0]) || fTmp[0].equals(tmp[1]))
						&& (!fTmp[1].equals(tmp[0]) || !fTmp[1].equals(tmp[1])))
					emitter.emit(ret, fTmp[1]);

				if ((fTmp[1].equals(tmp[0])
						|| fTmp[1].equals(tmp[1]) && (!fTmp[1].equals(tmp[0]) || !fTmp[1].equals(tmp[1]))))
					emitter.emit(ret, fTmp[0]);
			}
		}

	}

	@Override
	public String reduce(String key, String[] values) {
		StringBuilder stringBuilder = new StringBuilder();
		ArrayList<String> all = new ArrayList<String>(Arrays.asList(values));
		ArrayList<String> some = new ArrayList<>();
		stringBuilder.append(key + " # ");
		for (String friend : all) {
			if (Collections.frequency(all, friend) > 1 && !some.contains(friend)) {
				stringBuilder.append(friend + " ");
				some.add(friend);
			}
		}
		return stringBuilder.toString();

	}
}

class NumberOfTriangles implements Map<String, String>, Reduce<String, String> {

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
