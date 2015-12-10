package mapreduce;

public interface Reduce<K, V1, V2> {

    public V2 reduce(K key, V1 [] values);
    
}
