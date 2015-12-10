package mapreduce.worker;

import java.lang.reflect.Array;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mapreduce.KeyValue;
import mapreduce.Reduce;

public class ReduceWorker<K, V1, V2> extends Worker<K, V1 [], KeyValue<K, V2>> {

	private Reduce<K, V1, V2> reduceFunction = null;
	
	public ReduceWorker(BlockingQueue<KeyValue<K, V1[]>> taskQueue) {
		super(taskQueue);
	}
	
	public void setReduceFunctio(Reduce<K, V1, V2> reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	@Override
	public KeyValue<K, V2> doProcessing(KeyValue<K, V1[]> keyValue) {
        //ugly hack in order to cast task.getValue() array as its actual type
        Class c = keyValue.getValue()[0].getClass();
        V1 [] v = (V1 []) Array.newInstance(c, keyValue.getValue().length);
        for (int i = 0;i < v.length;i++) {
            v[i] = keyValue.getValue()[i];
        }
        //end of ugly hack
        
		return new KeyValue<K, V2>(keyValue.getKey(), reduceFunction.reduce(keyValue.getKey(), v));
	}

}
