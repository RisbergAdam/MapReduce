package mapreduce.worker;

import java.lang.reflect.Array;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mapreduce.KeyValue;
import mapreduce.Reduce;

public class ReduceWorker extends Worker {

	private Reduce reduceFunction = null;
	
	public ReduceWorker() {

	}
	
	public void setReduceFunctio(Reduce reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	@Override
	public Object doProcessing(KeyValue keyValue) {
        //ugly hack in order to cast task.getValue() array as its actual type
        Class c = ((Object []) keyValue.getValue())[0].getClass();
        Object [] v = (Object []) Array.newInstance(c, ((Object [])keyValue.getValue()).length);
        for (int i = 0;i < v.length;i++) {
            v[i] = ((Object []) keyValue.getValue())[i];
        }
        //end of ugly hack
        
		return new KeyValue(keyValue.getKey(), reduceFunction.reduce(keyValue.getKey(), v));
	}

}
