package mapreduce;

public class KeyValue<K, V> {

    private K key = null;
    private V value = null;
    
    public KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }
    
    public K getKey() {
        return key;
    }
    
    public V getValue() {
        return value;
    }
    
}
