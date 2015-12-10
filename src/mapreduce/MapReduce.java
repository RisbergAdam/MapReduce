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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import mapreduce.worker.MapWorker;
import mapreduce.worker.ReduceWorker;

//K1 V2 input types
//K2 V2 intermediate types
//V3 output type

public class MapReduce<K1, V1, K2, V2, V3> implements Emitter<K2, V2> {
    
	private BlockingQueue<KeyValue<K1, V1>> mapTaskQueue = new LinkedBlockingQueue<KeyValue<K1, V1>>();
	private BlockingQueue<KeyValue<K2, V2 []>> reduceTaskQueue = new LinkedBlockingQueue<KeyValue<K2, V2 []>>();
    
    private MapWorker<K1, V1, K2, V2> [] mapThreads = null;
    private ReduceWorker<K2, V2, V3> [] reduceThreads = null;

    private AbstractMap<K2, AbstractCollection<V2>> shuffleMap = new ConcurrentHashMap<K2, AbstractCollection<V2>>(300*300, 0.75f, 4);
    
    private long totalTime = 0;

    public MapReduce(int mapThreadCount, int reduceThreadCount) {
    	//stupid generic arrays
    	MapWorker<K1, V1, K2, V2> dummyMapThread = new MapWorker<>(mapTaskQueue, this);
    	ReduceWorker<K2, V2, V3> dummyReduceThread = new ReduceWorker<>(reduceTaskQueue);
        
        dummyMapThread.kill();
        dummyReduceThread.kill();
        
        mapThreads = (MapWorker<K1, V1, K2, V2> []) Array.newInstance(dummyMapThread.getClass(), mapThreadCount);
        reduceThreads = (ReduceWorker<K2, V2, V3> []) Array.newInstance(dummyReduceThread.getClass(), reduceThreadCount);
        
        for (int i = 0;i < mapThreads.length;i++) {
            mapThreads[i] = new MapWorker<K1, V1, K2, V2>(mapTaskQueue, this);
        }
        
        for (int i = 0;i < reduceThreads.length;i++) {
            reduceThreads[i] = new ReduceWorker<K2, V2, V3>(reduceTaskQueue);
        }
        
    }
    
    public void invoke(DataSource<K1, V1> source, Map<K1, V1, K2, V2> mapFunction, Reduce<K2, V2, V3> reduceFunction) {
    	applyMap(source, mapFunction);
    	applyReduce(reduceFunction);
    }
    
    public void killThreads() {
        for (MapWorker<K1, V1, K2, V2> t : mapThreads) {
            t.kill();
        }
        
        for (ReduceWorker<K2, V2, V3> t : reduceThreads) {
            t.kill();
        }
    }
    
    private void applyMap(DataSource<K1, V1> source, Map<K1, V1, K2, V2> mapFunction) {
        //read tasks from data source
        source.apply(new Emitter<K1, V1>() {
			public void emit(K1 key, V1 value) {
				mapTaskQueue.add(new KeyValue<K1, V1>(key, value));
			}
		});
    	
    	long startTime = System.currentTimeMillis();
        totalTime = startTime;
        
        //start map threads
        for (MapWorker<K1, V1, K2, V2> t : mapThreads) {
        	t.setMapFunction(mapFunction);
            t.startProcessing();
        }
        

        
        for (MapWorker<K1, V1, K2, V2> t : mapThreads) {
        	mapTaskQueue.add(new KeyValue<K1, V1>(null, null));
        }
        
        //wait for map threads to finish
        for (MapWorker<K1, V1, K2, V2> t : mapThreads) {
            t.waitForProcessing();
        }

        System.out.println("Mapping finished in " + (System.currentTimeMillis() - startTime) + " milliseconds");
    }
    
    private void applyReduce(Reduce<K2, V2, V3> reduceFunction) {
        long startTime = System.currentTimeMillis();
        
        KeyValue<K2, V2 []> [] shuffeled = shuffle();
        
        //keep this for future debugging purposes
        /*for (KeyValue<K, V []> kv : shuffeled) {
            System.out.print(kv.getKey() + ": ");
            for (V v : kv.getValue()) {
                System.out.print(v + ", ");
            }
            System.out.println("\n");
        }*/
        
        //fill reduce task queue
        for (KeyValue<K2, V2 []> kv : shuffeled) {
            reduceTaskQueue.add(kv);
        }
        
        //start reduce workers
        for (ReduceWorker<K2, V2, V3> t : reduceThreads) {
        	reduceTaskQueue.add(new KeyValue<K2, V2 []>(null, null));
        	t.setReduceFunctio(reduceFunction);
            t.startProcessing();
        }
        
        ArrayList<KeyValue<K2, V3>> resultList = new ArrayList<>();
        
        //wait for reduce workers to finish
        for (ReduceWorker<K2, V2, V3> t : reduceThreads) {
            resultList.addAll(t.waitForProcessing());
        }
        
        //write results to data sink
        //TODO
        
        
        System.out.println("Reducing finished in " + (System.currentTimeMillis() - startTime) + " milliseconds");
        System.out.println("MapReduce finished in " + (System.currentTimeMillis() - totalTime) + " milliseconds");
        
    }
    
    private KeyValue<K2, V2 []> [] shuffle() {	        
        //now convert hashmap into keyvalue array using stupid hacks
        //stupid generic arrays
        KeyValue<K2, V2 []> dummy = new KeyValue<K2, V2 []>(null, null);
        KeyValue<K2, V2 []> [] shuffle = (KeyValue<K2, V2 []> []) Array.newInstance(dummy.getClass(), shuffleMap.size());
        
        //dummy array so that ArrayList can be converted into plain array
        V2 [] dummyArray = (V2 []) new Object[1];
        
        int i = 0;
        for (K2 key : shuffleMap.keySet()) {
            V2 [] combinedValues = shuffleMap.get(key).toArray(dummyArray);
            shuffle[i] = new KeyValue<K2, V2 []>(key, combinedValues);
            
            i++;
        }
        
        return shuffle;
    }

	@Override
	public void emit(K2 key, V2 value) {
		
        //if (!shuffleMap.containsKey(key)) {
    	shuffleMap.putIfAbsent(key, new ConcurrentLinkedQueue<V2>());
        //}
        
        shuffleMap.get(key).add(value);
	}
    
}
