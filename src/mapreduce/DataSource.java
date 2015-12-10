package mapreduce;

public interface DataSource<K, V> {

	public void apply(Emitter<K, V> emitter);
	
}
