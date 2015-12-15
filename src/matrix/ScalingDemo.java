package matrix;

public class ScalingDemo {

    public static void main(String [] arg) {
        benchmark(1);
        benchmark(2);
    }
    
    private static void benchmark(int threads) {
        //initialize the MapReduce framework
        MatrixReduce matrixReduce = new MatrixReduce(threads, threads);
        
        //randomize input matrices
        Matrix largeMatrix1 = Matrix.randMatrix(700);
        Matrix largeMatrix2 = Matrix.randMatrix(700);
        
        long startTime = System.currentTimeMillis();
        
        //long multiplication
        matrixReduce.multiply(largeMatrix1, largeMatrix2);
        
        System.out.println("Time taken when multiplying 700x700 matrices with " + threads + " threads: " + (System.currentTimeMillis() - startTime));
        
        //tear down framework as before
        matrixReduce.dispose();
    }
    
}
