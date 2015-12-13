package matrix;

import java.util.ArrayList;

import mapreduce.KeyValue;
import mapreduce.MapReduce;
import matrix.operators.Addition;
import matrix.operators.Determinant;
import matrix.operators.Multiplication;
import matrix.operators.LowerTriangular;
import matrix.operators.UpperTriangular;

public class MatrixReduce extends MapReduce {

	public MatrixReduce(int mapThreadCount, int reduceThreadCount) {
		super(mapThreadCount, reduceThreadCount);
	}

	public Matrix add(Matrix a, Matrix b) {
		return invoke(new Addition(a, b));
	}
	
	public Matrix multiply(Matrix a, Matrix b) {
		return invoke(new Multiplication(a, b));
	}
	
	public Double determinant(Matrix m) {
		return invoke(new Determinant(m));
	}
	
	public Matrix triangular(Matrix m) {
		Matrix o = Matrix.getUnit(m.getValues().length);
		Matrix mTemp = m;
		
		for (int i = 0;i < m.getValues().length - 1;i++) {
			Set<Matrix, Matrix> intermediateTriangle = invoke(new LowerTriangular(mTemp, i));
			o = multiply(intermediateTriangle.v1, o);
			mTemp = intermediateTriangle.v2;
		}
		
		return o;
	}
	
	public Matrix diagonal(Matrix m) {
		Matrix o = Matrix.getUnit(m.getValues().length);
		Matrix mTemp = m;
		
		for (int i = 0;i < m.getValues().length - 1;i++) {
			Set<Matrix, Matrix> intermediateTriangle = invoke(new LowerTriangular(mTemp, i));
			o = multiply(intermediateTriangle.v1, o);
			mTemp = intermediateTriangle.v2;
		}
		
		for (int i = m.getValues().length - 1;i > 0;i--) {
			Set<Matrix, Matrix> intermediateTriangle = invoke(new UpperTriangular(mTemp, i));
			o = multiply(intermediateTriangle.v1, o);
			mTemp = intermediateTriangle.v2;
		}
		
		return o;
	}
	
	
	public Matrix inverse(Matrix m) {
		Matrix o = diagonal(m);
		Matrix mInt = multiply(o, m);
		
		int mSize = o.getValues().length;
		
		for (int y = 0;y < mSize;y++) {
			for (int x = 0;x < mSize;x++) {
				o.getValues()[y][x] /= mInt.getValues()[y][y];
			}
		}
		
		return o;
	}
	
}
