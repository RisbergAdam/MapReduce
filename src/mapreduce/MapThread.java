package mapreduce;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class MapThread <K, V> implements Runnable {
    
    private Semaphore workWait = new Semaphore(0);
    private Semaphore masterWait = new Semaphore(1);
    
    private Map<K, V> map = null;
    private ConcurrentLinkedQueue<KeyValue<String, String>> mapTaskQueue = null;
    
    private boolean isKill = false;

    public MapThread(ConcurrentLinkedQueue<KeyValue<String, String>> mapTaskQueue) {
        this.mapTaskQueue = mapTaskQueue;
        new Thread(this).start();
    }
    
    public void startMapper(Map<K, V> map) {
        this.map = map;
        try {
          
            workWait.release();
            masterWait.acquire();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void waitForMapper() {
        try {
            masterWait.acquire();
        } catch (Exception e) {
            e.printStackTrace();
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
                
                KeyValue<String, String> task = null;
                
                while ((task = mapTaskQueue.poll()) != null && !isKill) {
                    map.map(task.getKey(), task.getValue());
                }
                
                masterWait.release();
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

}
