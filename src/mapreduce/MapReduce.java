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

public class MapReduce implements Emitter {
    
	private BlockingQueue mapTaskQueue = null;//new LinkedBlockingQueue();
	private BlockingQueue reduceTaskQueue = null;//new LinkedBlockingQueue();
    
    private MapWorker [] mapThreads = null;
    private ReduceWorker [] reduceThreads = null;

    private ConcurrentHashMap<Object, AbstractCollection<Object>> shuffleMap = null;//new ConcurrentHashMap(300*300, 0.75f, 4);
    
    private long totalTime = 0;

    public MapReduce(int mapThreadCount, int reduceThreadCount) {
    	//stupid generic arrays
    	MapWorker dummyMapThread = new MapWorker(this);
    	ReduceWorker dummyReduceThread = new ReduceWorker();
        
        dummyMapThread.kill();
        dummyReduceThread.kill();
        
        mapThreads = (MapWorker []) Array.newInstance(dummyMapThread.getClass(), mapThreadCount);
        reduceThreads = (ReduceWorker []) Array.newInstance(dummyReduceThread.getClass(), reduceThreadCount);
        
        for (int i = 0;i < mapThreads.length;i++) {
            mapThreads[i] = new MapWorker(this);
        }
        
        for (int i = 0;i < reduceThreads.length;i++) {
            reduceThreads[i] = new ReduceWorker();
        }
        
    }
    
    public <K1, V1, K2, V2, V3, R> R invoke(
    		DataSource<K1, V1> source, 
    		Map<K1, V1, K2, V2> mapFunction, 
    		Reduce<K2, V2, V3> reduceFunction,
    		DataAssembly<K2, V3, R> assembly) {
    	
    	this.mapTaskQueue = new LinkedBlockingQueue<KeyValue<K1, V1>>();
    	this.reduceTaskQueue = new LinkedBlockingQueue<KeyValue<K2, V2 []>>();
    	this.shuffleMap = new ConcurrentHashMap(300*300, 0.75f, 4);
    	applyMap(source, mapFunction);
    	return assembly.assemble(applyReduce(reduceFunction));
    }
    
    public <K1, V1, K2, V2, V3, R, T extends DataSource<K1, V1> & Map<K1, V1, K2, V2> & Reduce<K2, V2, V3> & DataAssembly<K2, V3, R>> R invoke(T t) {
    	return invoke(t, t, t, t);
    }
    
    public void dispose() {
        for (MapWorker t : mapThreads) {
            t.kill();
        }
        
        for (ReduceWorker t : reduceThreads) {
            t.kill();
        }
    }
    
    private <K1, V1, K2, V2> void applyMap(DataSource<K1, V1> source, Map<K1, V1, K2, V2> mapFunction) {
        //start map threads
        for (MapWorker t : mapThreads) {
        	t.setMapFunction(mapFunction);
            t.startProcessing(mapTaskQueue);
        }
        
        //read tasks from data source
        source.apply(new Emitter<K1, V1>() {
			public void emit(K1 key, V1 value) {
				mapTaskQueue.add(new KeyValue<K1, V1>(key, value));
			}
		});
        
        //add sentinel nodes to indicate end of mapTaskQueue
        for (MapWorker t : mapThreads) {
        	mapTaskQueue.add(new KeyValue<K1, V1>(null, null));
        }
        
        //wait for map threads to finish
        for (MapWorker t : mapThreads) {
            t.waitForProcessing();
        }
    }
    
    private <K2, V2, V3> ArrayList<KeyValue<K2, V3>> applyReduce(Reduce<K2, V2, V3> reduceFunction) {
        KeyValue<K2, V2 []> [] shuffeled = shuffle();
        
        //keep this for future debugging purposes
        /*for (KeyValue<K2, V2 []> kv : shuffeled) {
            System.out.print(kv.getKey() + ": ");
            for (V2 v : kv.getValue()) {
                System.out.print(v + ", ");
            }
            System.out.println("\n");
        }*/
        
        //fill reduce task queue
        for (KeyValue<K2, V2 []> kv : shuffeled) {
            reduceTaskQueue.add(kv);
        }
        
        //start reduce workers
        for (ReduceWorker t : reduceThreads) {
        	reduceTaskQueue.add(new KeyValue<K2, V2 []>(null, null));
        	t.setReduceFunctio(reduceFunction);
            t.startProcessing(reduceTaskQueue);
        }
        
        ArrayList<KeyValue<K2, V3>> resultList = new ArrayList<>();
        
        //wait for reduce workers to finish
        for (ReduceWorker t : reduceThreads) {
            resultList.addAll(t.waitForProcessing());
        }
        
        return resultList;
    }
    
    private <K2, V2> KeyValue<K2, V2 []> [] shuffle() {	        
        //now convert hashmap into keyvalue array using stupid hacks
        //stupid generic arrays
        KeyValue<K2, V2 []> dummy = new KeyValue<K2, V2 []>(null, null);
        KeyValue<K2, V2 []> [] shuffle = (KeyValue<K2, V2 []> []) Array.newInstance(dummy.getClass(), shuffleMap.size());
        
        //dummy array so that ArrayList can be converted into plain array
        V2 [] dummyArray = (V2 []) new Object[1];
        
        int i = 0;
        for (Object okey : shuffleMap.keySet()) {
            K2 key = (K2) okey;
        	//V2 [] combinedValues = shuffleMap.get(key).toArray(dummyArray); sometimes toArray will return the same reference for all keys?
            V2 [] combinedValues = shuffleMap.get(key).toArray(dummyArray).clone();
        	
            shuffle[i] = new KeyValue<K2, V2 []>(key, combinedValues);
            
            i++;
        }
        
        return shuffle;
    }

	@Override
	public void emit(Object key, Object value) {
		
        //if (!shuffleMap.containsKey(key)) {
    	shuffleMap.putIfAbsent(key, new ConcurrentLinkedQueue<Object>());
        //}
        
        shuffleMap.get(key).add(value);
	}
    
}
