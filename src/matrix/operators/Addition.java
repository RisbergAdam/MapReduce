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

public class Addition implements
						DataSource<Integer, Set<Matrix, Matrix>>,
						Map<Integer, Set<Matrix, Matrix>, Integer, Set<Integer, Double []>>,
						Reduce<Integer, Set<Integer, Double []>, Matrix>,
						DataAssembly<Integer, Matrix, Matrix> {

	//matrices used by DataSource
	private Matrix a, b;
	
	public Addition(Matrix a, Matrix b) {
		this.a = a;
		this.b = b;
	}

	@Override
	public void map(Integer keyIn, Set<Matrix, Matrix> valueIn,
			Emitter<Integer, Set<Integer, Double[]>> emitter) {
		Double [] row = new Double[valueIn.v1.getValues().length];
		
		for (int i = 0;i < row.length;i++) {
			row[i] = valueIn.v1.getValues()[keyIn][i] + valueIn.v2.getValues()[keyIn][i]; 
		}
		
		emitter.emit(0, new Set<Integer, Double[]>(keyIn, row));
	}
	
	@Override
	public Matrix reduce(Integer key, Set<Integer, Double[]>[] values) {
		int matrixSize = values[0].v2.length;
		
		Double [][] m = new Double[matrixSize][];
		
		for (Set<Integer, Double []> v : values) {
			m[v.v1] = v.v2;
		}
		
		return new Matrix(m);
	}

	@Override
	public void apply(Emitter<Integer, Set<Matrix, Matrix>> emitter) {
		Set<Matrix, Matrix> mSet = new Set<Matrix, Matrix>(a, b);
		
		for (int i = 0;i < a.getValues().length;i++) {
				emitter.emit(i, mSet);
		}
	}

	@Override
	public Matrix assemble(
			ArrayList<KeyValue<Integer, Matrix>> values) {
		return values.get(0).getValue();
	}

}
