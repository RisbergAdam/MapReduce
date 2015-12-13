package matrix.operators;

import java.util.ArrayList;

import mapreduce.DataAssembly;
import mapreduce.DataSource;
import mapreduce.Emitter;
import mapreduce.KeyValue;
import mapreduce.Map;
import mapreduce.Reduce;
import matrix.Matrix;
import matrix.Set;

public class UpperTriangular implements
								DataSource<Set<Integer, Integer>, Matrix>,
								Map<Set<Integer, Integer>, Matrix, Integer, Set<Double [], Double []>>,
								Reduce<Integer, Set<Double [], Double []>, Set<Double [], Double []>>, 
								DataAssembly<Integer, Set<Double [], Double []>, Set<Matrix, Matrix>> {

	private Matrix m;
	private int row;
	
	public UpperTriangular(Matrix m, int row) {
		this.m = m;
		this.row = row;
		
		/*double firstValue = firstNonZero(m, row);
		
		if (firstValue != 1) {
			Matrix mc = m.clone();
			
			for (int x = 0;x < mc.getValues().length;x++) {
				mc.getValues()[row][x] = mc.getValues()[row][x]/firstValue; 
			}
			
			this.m = mc;
		}*/
		

	}

	@Override
	public void apply(Emitter<Set<Integer, Integer>, Matrix> emitter) {
		int mSize = m.getValues().length;

		/*for (int r = row + 1;r < mSize;r++) {
			emitter.emit(new Set<Integer, Integer>(row, r), m);
		}*/
		
		for (int r = 0;r < mSize;r++) {
			emitter.emit(new Set<Integer, Integer>(row, r), m);
		}
	}

	//key.v1 = source row
	//key.v2 = current working row
	@Override
	public void map(Set<Integer, Integer> key, Matrix m,
			Emitter<Integer, Set<Double [], Double []>> emitter) {
		
		Double [] nRow = new Double[m.getValues().length];
		Double [] nRowV = new Double[m.getValues().length];
		
		for (int i = 0;i < nRow.length;i++) {
			nRow[i] = 0.0;
		}
		nRow[key.v2] = 1.0;
		
		if (key.v2 >= key.v1 || m.getValues()[key.v2][key.v1] == 0) {
			emitter.emit(key.v2, new Set<Double[], Double[]>(nRow, m.getValues()[key.v2]));
			
			return;
		}
		
		nRow[key.v1] = -firstNonZero(m, key.v2)/firstNonZero(m, key.v1);
		
		for (int i = 0;i < nRow.length;i++) {
			nRowV[i] = m.getValues()[key.v2][i] + nRow[key.v1] * m.getValues()[key.v1][i];
		}
		
		
		emitter.emit(key.v2, new Set<Double[], Double[]>(nRow, nRowV));
	}
	
	private double firstNonZero(Matrix m, int row) {
		for (int x = 0;x < m.getValues().length;x++) {
			if (m.getValues()[row][m.getValues().length - x - 1] != 0) {
				return m.getValues()[row][m.getValues().length - x - 1];
			}
		}
		return 0;
	}

	@Override
	public Set<Double [], Double[]> reduce(Integer key, Set<Double [], Double[]>[] values) {
		return values[0];
	}
	
	@Override
	public Set<Matrix, Matrix> assemble(ArrayList<KeyValue<Integer, Set<Double [], Double[]>>> values) {
		Double [][] nMatrix = new Double[values.get(0).getValue().v1.length][];
		Double [][] nMatrixV = new Double[values.get(0).getValue().v1.length][];
		
		for (KeyValue<Integer, Set<Double [], Double[]>> v : values) {
			nMatrix[v.getKey()] = v.getValue().v1;
		}
		
		for (KeyValue<Integer, Set<Double [], Double[]>> v : values) {
			nMatrixV[v.getKey()] = v.getValue().v2;
		}
		
		return new Set<Matrix, Matrix>(new Matrix(nMatrix), new Matrix(nMatrixV));
	}

	
}