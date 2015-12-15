package matrix;

import java.util.Random;

public class Matrix {
	
	private Double [][] values = null;

	public Matrix(Double [][] values) {
		this.values = values;
	}

	public Double [][] getValues() {
		return values;
	}
	
	public double calculateDeterminant() {
		if (getValues().length == 2) {
			Double [][] v = getValues();
			return v[0][0]*v[1][1] - v[1][0]*v[0][1];
		} else {
			double determinant = 0;
			for (int i = 0;i < getValues().length;i++) {
				determinant += getValues()[0][i] * getMinor(i, 0).calculateDeterminant() * (i % 2 == 0 ? 1.0 : -1.0);
			}
			return determinant;
		}
	}
	
	public Matrix getMinor(int col, int row) {
		int mSize = getValues().length;
		Double [][] nMatrix = new Double[mSize-1][mSize-1];
		
		for (int y = 0;y < mSize - 1;y++) {
			for (int x = 0;x < mSize - 1;x++) {
				int dx = x + (x >= col ? 1 : 0);
				int dy = y + (y >= row ? 1 : 0);
				
				nMatrix[y][x] = getValues()[dy][dx];
			}
		}
		
		return new Matrix(nMatrix);
	}
	
	public Matrix transpose() {
		int mSize = getValues().length;
		Double [][] nMatrix = new Double[mSize][mSize];
		
		for (int x = 0;x < mSize;x++) {
			for (int y = 0;y < mSize;y++) {
				nMatrix[x][y] = getValues()[y][x];
			}
		}
		
		return new Matrix(nMatrix);
	}
	
	public Matrix clone() {
		Double [][] nMatrix = new Double[getValues().length][getValues().length];
		
		for (int y = 0;y < nMatrix.length;y++) {
			for (int x = 0;x < nMatrix.length;x++) {
				//nMatrix[x][y] = getValues()[x][y] will copy reference?
				double v = getValues()[x][y].doubleValue();
				nMatrix[x][y] = v;
			}
		}
		
		return new Matrix(nMatrix);
	}
	
	public String toString() {
		String r = "";
		
		for (int y = 0;y < values.length;y++) {
			r += "[" + round(values[y][0], 3);
			for (int x = 1;x < values[y].length;x++) {
				r += ", " + round(values[y][x], 3);
			}
			r += "]\n";
		}
		
		return r;
	}
	
	private double round(double d, int decimals) {
	    return ((double)Math.round(d * Math.pow(10, decimals)))/Math.pow(10, decimals);
	}
	
	public static Matrix getUnit(int size) {
		Double [][] nMatrix = new Double[size][size];
		
		for (int y = 0;y < size;y++) {
			for (int x = 0;x < size;x++) {
				nMatrix[x][y] = x == y ? 1.0 : 0.0;
			}
		}
		
		return new Matrix(nMatrix);
	}

	public static Matrix randMatrix(int size) {
		Random r = new Random();
		
		Double [][] m = new Double[size][size];
		
		for (int x = 0;x < size;x++) {
			for (int y = 0;y < size;y++) {
				m[x][y] = (double) r.nextInt(10) - 5;
			}
		}
		
		return new Matrix(m);
	}
	
}
