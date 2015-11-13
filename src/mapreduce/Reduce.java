package mapreduce;

public interface Reduce<K, V> {

    public String reduce(K key, V [] values);
    
}
