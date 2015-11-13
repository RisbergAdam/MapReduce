package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Master<K, V> {
    
    private ConcurrentLinkedQueue<KeyValue<String, String>> mapTaskQueue = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<KeyValue<K, V []>> reduceTaskQueue = new ConcurrentLinkedQueue<>();
    
    private MapThread<K, V> [] mapThreads = null;
    private ReduceThread<K, V> [] reduceThreads = null;
    
    
    private ArrayList<KeyValue<K, V>> emits = new ArrayList<>();
    
    public Master(int mapThreadCount, int reduceThreadCount) {
        MapThread<K, V> dummyMapThread = new MapThread<>(mapTaskQueue);
        ReduceThread<K, V> dummyReduceThread = new ReduceThread<>(reduceTaskQueue);
        
        dummyMapThread.kill();
        dummyReduceThread.kill();
        
        mapThreads = (MapThread<K, V> []) Array.newInstance(dummyMapThread.getClass(), mapThreadCount);
        reduceThreads = (ReduceThread<K, V> []) Array.newInstance(dummyReduceThread.getClass(), reduceThreadCount);
        
        for (int i = 0;i < mapThreads.length;i++) {
            mapThreads[i] = new MapThread<K, V>(mapTaskQueue);
        }
        
        for (int i = 0;i < reduceThreads.length;i++) {
            reduceThreads[i] = new ReduceThread<K, V>(reduceTaskQueue);
        }
    }
    
    public void applyMap(Map<K, V> mapper) {
        System.out.println("apply map");
        
        File [] mapTaskFiles = new File("input").listFiles();
        mapper.setMaster(this);
        
        //add all tasks to mapQueue
        for (File f : mapTaskFiles) {
            if (f.isDirectory()) continue;
            String fileName = f.getName();
            String fileContent = readFileContent(f);
            
            mapTaskQueue.add(new KeyValue<String, String>(fileName, fileContent));
        }
        
        System.out.println("tasks created");
        
        //start map threads
        for (MapThread<K, V> t : mapThreads) {
            t.startMapper(mapper);
        }
        
        System.out.println("mappers started");
        
        //wait for map threads to finish
        for (MapThread<K, V> t : mapThreads) {
            t.waitForMapper();
        }
        
        System.out.println("all mappers finished!");
    }
    
    private String readFileContent(File f) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(f));
            StringBuilder builder = new StringBuilder();
            String s = "";
            String nLine = "";
            
            while ((s = reader.readLine()) != null) {
                builder.append(nLine);
                builder.append(s);
                nLine = "\n";
            }
            
            reader.close();
            
            return builder.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public void applyReduce(Reduce<K, V> reduce) {
        System.out.println("applyReduce");
        
        KeyValue<K, V []> [] shuffeled = shuffle();
        
        //keep this for future debugging purposes
        /*for (KeyValue<K, V []> kv : shuffeled) {
            System.out.print(kv.getKey() + ": ");
            for (V v : kv.getValue()) {
                System.out.print(v + ", ");
            }
            System.out.println("\n");
        }*/
        
        //fill reduce task queue
        for (KeyValue<K, V []> kv : shuffeled) {
            reduceTaskQueue.add(kv);
        }
        
        System.out.println("reduce queue filled");
        
        //start reduce workers
        for (ReduceThread<K, V> t : reduceThreads) {
            t.startReducer(reduce);
        }
        
        System.out.println("reduce workers started");
        
        ArrayList<KeyValue<K, String>> resultList = new ArrayList<>();
        
        //wait for reduce workers to finish
        for (ReduceThread<K, V> t : reduceThreads) {
            resultList.addAll(t.waitForReducer());
        }
        
        System.out.println("reduce workers finished");
        
        //write results to a file
        try {
            File outFile = new File("output/output.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
            
            for (KeyValue<K, String> r : resultList) {
                writer.write("" + r.getKey() + ": " + r.getValue() + "\n");
            }
            
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    private KeyValue<K, V []> [] shuffle() {
        
        HashMap<K, ArrayList<V>> shuffleMap = new HashMap<>();
        
        for (KeyValue<K, V> kv : emits) {
            if (!shuffleMap.containsKey(kv.getKey())) {
                shuffleMap.put(kv.getKey(), new ArrayList<V>());
            }
            
            shuffleMap.get(kv.getKey()).add(kv.getValue());
        }
        
        KeyValue<K, V []> dummy = new KeyValue<K, V []>(null, null);
        
        KeyValue<K, V []> [] shuffle = (KeyValue<K, V []> []) Array.newInstance(dummy.getClass(), shuffleMap.size());
        
        
        V [] dummyArray = (V []) new Object[1];
        
        int i = 0;
        for (K key : shuffleMap.keySet()) {
            V [] combinedValues = shuffleMap.get(key).toArray(dummyArray);
            shuffle[i] = new KeyValue<K, V []>(key, combinedValues);
            
            i++;
        }
        
        return shuffle;
    }

    public void emit(K key, V value) {
        emits.add(new KeyValue<K, V>(key, value));
    }
    
}
