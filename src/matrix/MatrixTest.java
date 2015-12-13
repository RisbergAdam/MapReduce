package matrix;

public class MatrixTest {

	public static void main(String [] arg) {
		Double [][] d1 = {{1.0, 3.0, -3.0}, {-2.0, 1.0, 1.0}, {-3.0, 0.0, 1.0}};
		Double [][] d2 = {{1.0, 0.0, 0.0}, {0.0, 1.0, 0.0}, {0.0, -1.0/3.0, 1.0}};
		Matrix m1 = new Matrix(d1);
		Matrix m2 = new Matrix(d2);
		Matrix m = Matrix.randMatrix(1000);
		MatrixReduce mp = new MatrixReduce(1, 1);
		
		long sTime = System.currentTimeMillis();
		
		System.out.println("total: " + (System.currentTimeMillis() - sTime));
		
		mp.dispose();
	}
	
}
