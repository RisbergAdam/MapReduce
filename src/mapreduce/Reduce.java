package mapreduce;

public interface Reduce<K, V> {

    public KeyValue<K, String> reduce(K key, V [] values);
    
}
