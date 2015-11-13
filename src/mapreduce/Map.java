package mapreduce;

public interface Map<K, V> {
    
	public void map(String fileName, String fileContent, Emitter<K, V> emitter);
    
}
