package mapreduce;

public interface Map<K1, V1, K2, V2> {
    
	public void map(K1 keyIn, V1 valueIn, Emitter<K2, V2> emitter);
    
}
