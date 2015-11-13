package mapreduce.worker;

import java.util.concurrent.ConcurrentLinkedQueue;

import mapreduce.Emitter;
import mapreduce.KeyValue;
import mapreduce.Map;

public class MapWorker<K, V> extends Worker<String, String, Void>{

	private Map<K, V> mapFunction = null;
	private Emitter<K, V> emitter = null;
	
	public MapWorker(ConcurrentLinkedQueue<KeyValue<String, String>> reduceTaskQueue, Emitter<K, V> emitter) {
		super(reduceTaskQueue);
		this.emitter = emitter;
	}
	
	public void setMapFunction(Map<K, V> mapFunction) {
		this.mapFunction = mapFunction;
	}

	@Override
	public Void doProcessing(KeyValue<String, String> keyValue) {
		mapFunction.map(keyValue.getKey(), keyValue.getValue(), emitter);
		return null;
	}

}
