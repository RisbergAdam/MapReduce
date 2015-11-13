package mapreduce;

public abstract class Map<K, V> {
    
    private Master<K, V> master = null;
    
    public Map(Master<K, V> master) {
        this.master = master;
    }
    
    public abstract void map(String fileName, String fileContent);
    
    public void emit(K key, V value) {
        master.emit(key, value);
    }
    
}
