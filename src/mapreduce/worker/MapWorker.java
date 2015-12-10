package mapreduce.worker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mapreduce.Emitter;
import mapreduce.KeyValue;
import mapreduce.Map;

public class MapWorker<K1, V1, K2, V2> extends Worker<K1, V1, Void>{

	private Map<K1, V1, K2, V2> mapFunction = null;
	private Emitter<K2, V2> emitter = null;
	
	public MapWorker(BlockingQueue<KeyValue<K1, V1>> taskQueue, Emitter<K2, V2> emitter) {
		super(taskQueue);
		this.emitter = emitter;
	}
	
	public void setMapFunction(Map<K1, V1, K2, V2> mapFunction) {
		this.mapFunction = mapFunction;
	}

	@Override
	public Void doProcessing(KeyValue<K1, V1> keyValue) {
		mapFunction.map(keyValue.getKey(), keyValue.getValue(), emitter);
		return null;
	}

}
