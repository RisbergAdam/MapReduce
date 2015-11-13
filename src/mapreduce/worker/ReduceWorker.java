package mapreduce.worker;

import java.lang.reflect.Array;
import java.util.concurrent.ConcurrentLinkedQueue;

import mapreduce.KeyValue;
import mapreduce.Reduce;

public class ReduceWorker<K, V> extends Worker<K, V [], KeyValue<K, String>> {

	private Reduce<K, V> reduceFunction = null;
	
	public ReduceWorker(ConcurrentLinkedQueue<KeyValue<K, V[]>> reduceTaskQueue) {
		super(reduceTaskQueue);
	}
	
	public void setReduceFunctio(Reduce<K, V> reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	@Override
	public KeyValue<K, String> doProcessing(KeyValue<K, V[]> keyValue) {
        //ugly hack in order to cast task.getValue() array as its actual type
        Class c = keyValue.getValue()[0].getClass();
        V [] v = (V []) Array.newInstance(c, keyValue.getValue().length);
        for (int i = 0;i < v.length;i++) {
            v[i] = keyValue.getValue()[i];
        }
        //end of ugly hack
        
		return new KeyValue<K, String>(keyValue.getKey(), reduceFunction.reduce(keyValue.getKey(), v));
	}

}
