package mapreduce.solutions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import mapreduce.Emitter;
import mapreduce.Map;
import mapreduce.Reduce;

public class CommonFriends implements Map<String, String>, Reduce<String, String> {

	public static void main(String [] args) {
		CommonFriends cf = new CommonFriends();
		SolutionUtils.MapReduce(cf, cf, SolutionUtils.parse("CommonFriends", args));
	}
	
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