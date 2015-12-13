package matrix;

public class MatrixBenchmark {

	public static void main(String [] arg) {
		
		int mapThreadCount = 0, reduceThreadCount = 0;
		
		try {
			
			mapThreadCount = Integer.parseInt(arg[0]);
			reduceThreadCount = Integer.parseInt(arg[1]);
			
		} catch (Exception e) {
			System.out.println("usage: MatrixBenchmark <mapThreads> <reduceThreads>");
			return;
		}
		
		MatrixReduce matrixReduce = new MatrixReduce(mapThreadCount, reduceThreadCount);
		
		benchAdd(matrixReduce);
		benchMultiply(matrixReduce);
		benchTriangulation(matrixReduce);
		benchDeterminant(matrixReduce);
		benchInverse(matrixReduce);
		
		matrixReduce.dispose();
	}
	
	private static void benchAdd(MatrixReduce matrixReduce) {
		//warmup
		Matrix warmupMatrix = Matrix.randMatrix(5);
		for (int i = 0;i < 1000;i++) {
			matrixReduce.add(warmupMatrix, warmupMatrix);
		}
		
		//if benchmark takes too long, reduce matrix-size here
		Matrix testMatrix1 = Matrix.randMatrix(1000);
		Matrix testMatrix2 = Matrix.randMatrix(1000);
		
		long time = System.currentTimeMillis();
		int attempts = 100;
		for (int i = 0;i < attempts;i++) {
			matrixReduce.add(testMatrix1, testMatrix2);
		}
		
		System.out.println("addition 1000x1000: " + (System.currentTimeMillis()-time)/(double)attempts + " ms per operation");
	}
	
	private static void benchMultiply(MatrixReduce matrixReduce) {
		//warmup
		Matrix warmupMatrix = Matrix.randMatrix(5);
		for (int i = 0;i < 1000;i++) {
			matrixReduce.multiply(warmupMatrix, warmupMatrix);
		}
		
		//if benchmark takes too long, reduce matrix-size here
		Matrix testMatrix1 = Matrix.randMatrix(700);
		Matrix testMatrix2 = Matrix.randMatrix(700);
		
		long time = System.currentTimeMillis();
		int attempts = 5;
		for (int i = 0;i < attempts;i++) {
			matrixReduce.multiply(testMatrix1, testMatrix2);
		}
		
		System.out.println("multiplication 700x700: " + (System.currentTimeMillis()-time)/(double)attempts + " ms per operation");
	}
	
	private static void benchTriangulation(MatrixReduce matrixReduce) {
		//warmup
		Matrix warmupMatrix = Matrix.randMatrix(5);
		for (int i = 0;i < 1000;i++) {
			matrixReduce.triangular(warmupMatrix);
		}
		
		//if benchmark takes too long, reduce matrix-size here
		Matrix testMatrix = Matrix.randMatrix(200);
		
		long time = System.currentTimeMillis();
		int attempts = 5;
		for (int i = 0;i < attempts;i++) {
			matrixReduce.triangular(testMatrix);
		}
		
		System.out.println("triangulation 200x200: " + (System.currentTimeMillis()-time)/(double)attempts + " ms per operation");
	}
	
	private static void benchDeterminant(MatrixReduce matrixReduce) {
		//warmup
		Matrix warmupMatrix = Matrix.randMatrix(4);
		for (int i = 0;i < 1000;i++) {
			matrixReduce.determinant(warmupMatrix);
		}
		
		//if benchmark takes too long, reduce matrix-size here
		Matrix testMatrix = Matrix.randMatrix(11);
		
		long time = System.currentTimeMillis();
		int attempts = 5;
		for (int i = 0;i < attempts;i++) {
			matrixReduce.determinant(testMatrix);
		}
		
		System.out.println("determinant 11x11: " + (System.currentTimeMillis()-time)/(double)attempts + " ms per operation");
	}
	
	private static void benchInverse(MatrixReduce matrixReduce) {
		//warmup
		Matrix warmupMatrix = Matrix.randMatrix(10);
		for (int i = 0;i < 1000;i++) {
			matrixReduce.inverse(warmupMatrix);
		}
		
		//if benchmark takes too long, reduce matrix-size here
		Matrix testMatrix = Matrix.randMatrix(130);
		
		long time = System.currentTimeMillis();
		int attempts = 5;
		for (int i = 0;i < attempts;i++) {
			matrixReduce.inverse(testMatrix);
		}
		
		System.out.println("inverse 130x130: " + (System.currentTimeMillis()-time)/(double)attempts + " ms per operation");
	}
	
}
