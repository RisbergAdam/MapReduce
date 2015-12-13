package mapreduce.worker;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import mapreduce.KeyValue;
import mapreduce.Reduce;

public abstract class Worker implements Runnable {
    
    private Semaphore workWait = new Semaphore(0);
    private Semaphore masterWait = new Semaphore(1);
    
    private BlockingQueue<KeyValue> taskQueue = null;
    private ArrayList result = null;
    
    private boolean isKill = false;

    public Worker() {
        new Thread(this).start();
    }
    
    public <K, V, R> void startProcessing(BlockingQueue<KeyValue> taskQueue) {
    	this.taskQueue = taskQueue;
    	this.result = new ArrayList<R>();
        try {
            workWait.release();
            masterWait.acquire();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public ArrayList waitForProcessing() {
        try {
            masterWait.acquire();
            masterWait.release();
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
                
                //poll queue until null then go back to idle
                result.clear();
                
                KeyValue task = null;
                
                while ((task = taskQueue.take()).getKey() != null && !isKill) {
                    Object r = doProcessing(task);
                    if (r == null) continue;
                    result.add(r);
                }
                
                masterWait.release();
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    public abstract Object doProcessing(KeyValue keyValue);

}