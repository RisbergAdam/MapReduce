package mapreduce;

import java.util.ArrayList;

import matrix.Set;

public interface DataAssembly<K, V, R> {

	public R assemble(ArrayList<KeyValue<K, V>> values);
	
}
