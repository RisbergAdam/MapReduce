package mapreduce;

public interface Emitter<K, V> {

	public void emit(K key, V value);
	
}
