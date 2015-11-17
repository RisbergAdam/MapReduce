package mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class Example {

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



		
		
		
		MapReduce<String, String> master = new MapReduce<>(mapThreads, reduceThreads);
		master.applyMap(new NumberOfTrianglesMap(), input);
		master.applyReduce(new NumberOfTrianglesReduce(), output);
		master.killThreads();
	}

}

class TestMap implements Map<String, Integer> {

	@Override
	public void map(String fileName, String fileContent, Emitter<String, Integer> emitter) {
		fileContent = fileContent.replace("\n", " ").replace("\r", " ");
		String[] words = fileContent.split(" ");
		for (String word : words) {
			emitter.emit(word.toLowerCase().trim(), 1);
		}
	}

}

class TestReduce implements Reduce<String, Integer> {

	public String reduce(String key, Integer[] values) {
		int total = 0;

		for (Integer i : values) {
			total += i;
		}

		return "" + total;
	}
}

class GraphConversionMap implements Map<String, String> {

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

class GraphConversionReduce implements Reduce<String, String> {

	@Override
	public String reduce(String key, String[] values) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(key + " # ");
		for (String val : values) {
			stringBuilder.append(val + " ");
		}

		return stringBuilder.toString();
	}

}

class CommonFriendsMap implements Map<String, String> {

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
}

class CommonFriendsReduce implements Reduce<String, String> {

	@Override
	public String reduce(String key, String[] values) {
		StringBuilder stringBuilder = new StringBuilder();
		System.out.println(values.length);
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

class NumberOfTrianglesMap implements Map<String, String> {

	@Override
	public void map(String fileName, String fileContent, Emitter<String, String> emitter) {
		String[] words = fileContent.split("\n");
		for (int i = 0; i < words.length; i++) {
			words[i] = words[i].replace("(", "").replace(")", "");
		}
		String prev = "";
		for (String word : words) {
			String[] tmp = word.split(",");
			if (tmp[0].equals(prev))
				continue;
			prev = tmp[0];
			for (String wordInner : words) {
				emitter.emit(tmp[0], wordInner);
			}
		}

	}
}

class NumberOfTrianglesReduce implements Reduce<String, String> {

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
