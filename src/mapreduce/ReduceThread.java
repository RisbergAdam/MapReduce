package mapreduce;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class ReduceThread <K, V> implements Runnable {
    
    private Semaphore workWait = new Semaphore(0);
    private Semaphore masterWait = new Semaphore(1);
    
    private Reduce<K, V> reduce = null;
    private ConcurrentLinkedQueue<KeyValue<K, V []>> reduceTaskQueue = null;
    private ArrayList<KeyValue<K, String>> result = new ArrayList<>();
    
    private boolean isKill = false;

    public ReduceThread(ConcurrentLinkedQueue<KeyValue<K, V []>> reduceTaskQueue) {
        this.reduceTaskQueue = reduceTaskQueue;
        new Thread(this).start();
    }
    
    public void startReducer(Reduce<K, V> reduce) {
        this.reduce = reduce;
        try {
          
            workWait.release();
            masterWait.acquire();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public ArrayList<KeyValue<K, String>> waitForReducer() {
        try {
            masterWait.acquire();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public void kill() {
        isKill = true;
        workWait.release();
    }
    
    @Override
    public void run() {
        
        try {
            
            while (!isKill) {
                workWait.acquire();
                
                if (isKill) break;
                
                //poll queue until null
                result.clear();
                
                KeyValue<K, V []> task = null;
                
                while ((task = reduceTaskQueue.poll()) != null && !isKill) {
                    
                    //ugly hack in order to cast task.getValue() array as its actual type
                    Class c = task.getValue()[0].getClass();
                    V [] v = (V []) Array.newInstance(c, task.getValue().length);
                    for (int i = 0;i < v.length;i++) {
                        v[i] = task.getValue()[i];
                    }
                    //end of ugly hack
                    
                    KeyValue<K, String> r = reduce.reduce(task.getKey(), v);
                    result.add(r);
                }
                
                masterWait.release();
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

}
