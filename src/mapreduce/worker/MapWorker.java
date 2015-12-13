package mapreduce.worker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mapreduce.Emitter;
import mapreduce.KeyValue;
import mapreduce.Map;

public class MapWorker extends Worker{

	private Map mapFunction = null;
	private Emitter emitter = null;
	
	public MapWorker(Emitter emitter) {
		this.emitter = emitter;
	}
	
	public void setMapFunction(Map mapFunction) {
		this.mapFunction = mapFunction;
	}

	@Override
	public Void doProcessing(KeyValue keyValue) {
		mapFunction.map(keyValue.getKey(), keyValue.getValue(), emitter);
		return null;
	}

}
