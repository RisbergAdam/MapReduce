package mapreduce;

public class MRTest {

    public static void main(String [] arg) {
        Master<String, Integer> master = new Master<>(1, 1);
        master.applyMap(new TestMap());
        master.applyReduce(new TestReduce());
    }
    
}

class TestMap extends Map<String, Integer> {

    @Override
    public void map(String fileName, String fileContent) {
        System.out.println(fileName);
        String [] words = fileContent.split(" ");
        
        for (String word : words) {
            emit(word, 1);
        }
    }
    
}

class TestReduce implements Reduce<String, Integer> {

    public KeyValue<String, String> reduce(String key, Integer [] values) {
        int total = 0;
        
        for (Integer i : values) {
            total += i;
        }
        
        return new KeyValue<String, String>(key, "" + total);
    }
    
}