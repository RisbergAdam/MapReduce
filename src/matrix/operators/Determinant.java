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

public class Determinant implements 
							DataSource<Integer, Matrix>,
							Map<Integer, Matrix, Integer, Double>,
							Reduce<Integer, Double, Double>, 
							DataAssembly<Integer, Double, Double> {
	
	private Matrix matrix = null;

	public Determinant(Matrix matrix) {
		this.matrix = matrix;
	}
	
	@Override
	public void apply(Emitter<Integer, Matrix> emitter) {
		if (matrix.getValues().length == 2) {
			emitter.emit(0, matrix);
			return;
		}
		
		for (int i = 0;i < matrix.getValues().length;i++) {
			emitter.emit(i, matrix);
		}
	}
	

	@Override
	public void map(Integer column, Matrix matrix,
			Emitter<Integer, Double> emitter) {
		
		if (matrix.getValues().length == 2) {
			emitter.emit(0, matrix.calculateDeterminant());
			return;
		}
		
		double determinant = matrix.getValues()[0][column] * matrix.getMinor(column, 0).calculateDeterminant() * (column % 2 == 0 ? 1.0 : -1.0);
		
		emitter.emit(0, determinant);
	}

	
	@Override
	public Double reduce(Integer key, Double[] values) {
		double sum = 0;
		
		for (Double d : values) {
			sum += d;
		}
		
		return sum;
	}

	@Override
	public Double assemble(ArrayList<KeyValue<Integer, Double>> values) {
		return values.get(0).getValue();
	}

}
