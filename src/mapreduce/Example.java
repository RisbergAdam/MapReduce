package mapreduce;

public class Example {

    public static void main(String [] arg) {
        MapReduce<String, Integer> master = new MapReduce<>(1, 1);
        master.applyMap(new TestMap(), "input");
        master.applyReduce(new TestReduce(), "output");
        master.killThreads();
    }
    
}

class TestMap implements Map<String, Integer> {

    @Override
    public void map(String fileName, String fileContent, Emitter<String, Integer> emitter) {
        String [] words = fileContent.split(" ");
        
        for (String word : words) {
            emitter.emit(word.toLowerCase().trim(), 1);
        }
    }
    
}

class TestReduce implements Reduce<String, Integer> {

    public String reduce(String key, Integer [] values) {
        int total = 0;
        
        for (Integer i : values) {
            total += i;
        }
        
        return "" + total;
    }
    
}
