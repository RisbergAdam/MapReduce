package mapreduce.worker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mapreduce.Emitter;
import mapreduce.KeyValue;
import mapreduce.Map;

public class MapWorker<K, V> extends Worker<String, String, Void>{

	private Map<K, V> mapFunction = null;
	private Emitter<K, V> emitter = null;
	
	public MapWorker(BlockingQueue<KeyValue<String, String>> taskQueue, Emitter<K, V> emitter) {
		super(taskQueue);
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
