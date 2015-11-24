package mapreduce.solutions;
import mapreduce.Map;
import mapreduce.MapReduce;
import mapreduce.Reduce;

public class SolutionUtils {

	public final int mapThreadCount, reduceThreadCount;
	public final String inputDir, outputDir;
	
	private SolutionUtils(int mapThreadCount, int reduceThreadCount, String inputDir, String outputDir) {
		this.mapThreadCount = mapThreadCount;
		this.reduceThreadCount = reduceThreadCount;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
	}
	
	public static SolutionUtils parse(String programName, String [] args) {
		String usage = "Usage: java " + programName + " mapThreadCount reduceThreadCount inputDir outputDir";
		
        if (args.length != 4) {
            System.err.println(usage);
            System.exit(1);
        }
        
        int mapThreadCount = -1, reduceThreadCount = -1;
        
        try {
            mapThreadCount = Integer.parseInt(args[0]);
            reduceThreadCount = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Arguments must be an integer s");
            System.err.println(usage);
            System.exit(1);
        }
        
        return new SolutionUtils(mapThreadCount, reduceThreadCount, args[2], args[3]);
	}
	
	public static <K, V> void MapReduce(Map<K, V> map, Reduce<K, V> reduce, SolutionUtils args) {
		MapReduce<K, V> m = new MapReduce<K, V>(args.mapThreadCount, args.reduceThreadCount);
		m.applyMap(map, args.inputDir);
		m.applyReduce(reduce, args.outputDir);
		m.killThreads();
	}
	
}
