package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Array;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import mapreduce.worker.MapWorker;
import mapreduce.worker.ReduceWorker;

public class MapReduce<K, V> implements Emitter<K, V> {
    
	private BlockingQueue<KeyValue<String, String>> mapTaskQueue = new LinkedBlockingQueue<KeyValue<String,String>>();
	private BlockingQueue<KeyValue<K, V []>> reduceTaskQueue = new LinkedBlockingQueue<KeyValue<K, V []>>();
    
    private MapWorker<K, V> [] mapThreads = null;
    private ReduceWorker<K, V> [] reduceThreads = null;

    private AbstractMap<K, AbstractCollection<V>> shuffleMap = new ConcurrentHashMap<>();
    
    private long totalTime = 0;

    public MapReduce(int mapThreadCount, int reduceThreadCount) {
    	//stupid generic arrays
    	MapWorker<K, V> dummyMapThread = new MapWorker<>(mapTaskQueue, this);
    	ReduceWorker<K, V> dummyReduceThread = new ReduceWorker<>(reduceTaskQueue);
        
        dummyMapThread.kill();
        dummyReduceThread.kill();
        
        mapThreads = (MapWorker<K, V> []) Array.newInstance(dummyMapThread.getClass(), mapThreadCount);
        reduceThreads = (ReduceWorker<K, V> []) Array.newInstance(dummyReduceThread.getClass(), reduceThreadCount);
        
        for (int i = 0;i < mapThreads.length;i++) {
            mapThreads[i] = new MapWorker<K, V>(mapTaskQueue, this);
        }
        
        for (int i = 0;i < reduceThreads.length;i++) {
            reduceThreads[i] = new ReduceWorker<K, V>(reduceTaskQueue);
        }
        
    }
    
    public void killThreads() {
        for (MapWorker<K, V> t : mapThreads) {
            t.kill();
        }
        
        for (ReduceWorker<K, V> t : reduceThreads) {
            t.kill();
        }
    }
    
    public void applyMap(Map<K, V> mapFunction, String inputDirectory) {
    	long startTime = System.currentTimeMillis();
        totalTime = startTime;
    	
        File [] mapTaskFiles = new File(inputDirectory).listFiles();
        
        //start map threads
        for (MapWorker<K, V> t : mapThreads) {
        	t.setMapFunction(mapFunction);
            t.startProcessing();
        }
        
        //read tasks from provided directory into mapTaskQueue
        for (File f : mapTaskFiles) {
            if (f.isDirectory()) continue;
            String fileName = f.getName();
            String fileContent = readFileContent(f);
            
            mapTaskQueue.add(new KeyValue<String, String>(fileName, fileContent));
        }
        
        for (MapWorker<K, V> t : mapThreads) {
        	mapTaskQueue.add(new KeyValue<String, String>(null, null));
        }
        
        //wait for map threads to finish
        for (MapWorker<K, V> t : mapThreads) {
            t.waitForProcessing();
        }

        System.out.println("Mapping finished in " + (System.currentTimeMillis() - startTime) + " milliseconds");
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
    
    public void applyReduce(Reduce<K, V> reduceFunction, String output) {
        long startTime = System.currentTimeMillis();
        
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
        
        //start reduce workers
        for (ReduceWorker<K, V> t : reduceThreads) {
        	reduceTaskQueue.add(new KeyValue<K, V []>(null, null));
        	t.setReduceFunctio(reduceFunction);
            t.startProcessing();
        }
        
        ArrayList<KeyValue<K, String>> resultList = new ArrayList<>();
        
        //wait for reduce workers to finish
        for (ReduceWorker<K, V> t : reduceThreads) {
            resultList.addAll(t.waitForProcessing());
        }
        
        //write results to a file
        try {
            File outFile = new File(output + "/output.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
            
            for (KeyValue<K, String> r : resultList) {
                writer.write(r.getValue() + "\n");
            }
            
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        System.out.println("Reducing finished in " + (System.currentTimeMillis() - startTime) + " milliseconds");
        System.out.println("MapReduce finished in " + (System.currentTimeMillis() - totalTime) + " milliseconds");
        
    }
    
    private KeyValue<K, V []> [] shuffle() {	        
        //now convert hashmap into keyvalue array using stupid hacks
        //stupid generic arrays
        KeyValue<K, V []> dummy = new KeyValue<K, V []>(null, null);
        KeyValue<K, V []> [] shuffle = (KeyValue<K, V []> []) Array.newInstance(dummy.getClass(), shuffleMap.size());
        
        //dummy array so that ArrayList can be converted into plain array
        V [] dummyArray = (V []) new Object[1];
        
        int i = 0;
        for (K key : shuffleMap.keySet()) {
            V [] combinedValues = shuffleMap.get(key).toArray(dummyArray);
            shuffle[i] = new KeyValue<K, V []>(key, combinedValues);
            
            i++;
        }
        
        return shuffle;
    }

	@Override
	public void emit(K key, V value) {
        if (!shuffleMap.containsKey(key)) {
        	shuffleMap.put(key, new ConcurrentLinkedQueue<V>());
        }
        
        shuffleMap.get(key).add(value);
	}
    
}
