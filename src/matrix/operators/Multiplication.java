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

public class Multiplication implements 
								DataSource<Set<Integer, Integer>, Set<Matrix, Matrix>>,								
								Map<Set<Integer, Integer>, Set<Matrix, Matrix>, Integer, Set<Integer, Double>>,
								Reduce<Integer, Set<Integer, Double>, Double []>,
								DataAssembly<Integer, Double [], Matrix> {

	//matrices used by DataSource
    private Matrix a, b;
									
	public Multiplication(Matrix a, Matrix b) {
		this.a = a;
		this.b = b;
	}
									
	@Override
	public void map(Set<Integer, Integer> key, Set<Matrix, Matrix> mIn,
			Emitter<Integer, Set<Integer, Double>> emitter) {
		
		double sum = 0;
		
		for (int x = 0;x < mIn.v1.getValues().length;x++) {
			sum += mIn.v1.getValues()[key.v1][x] * mIn.v2.getValues()[x][key.v2];
		}
		
		emitter.emit(key.v1, new Set<Integer, Double>(key.v2, sum));
	}

	@Override
	public Double [] reduce(Integer key, Set<Integer, Double> [] values) {
		Double [] row = new Double[values.length];
		
		for (Set<Integer, Double> s : values) {
			row[s.v1] = s.v2;
		}
		
		return row;
	}
	
	@Override
	public void apply(
			Emitter<Set<Integer, Integer>, Set<Matrix, Matrix>> emitter) {
		Set<Matrix, Matrix> mSet = new Set<Matrix, Matrix>(a, b);
		
		for (int i = 0;i < a.getValues().length;i++) {
			for (int j = 0;j < a.getValues()[0].length;j++) {
				emitter.emit(new Set<Integer, Integer>(i, j), mSet);
			}
		}
	}

	@Override
	public Matrix assemble(ArrayList<KeyValue<Integer, Double[]>> values) {
		Double [][] resultMatrix = new Double[values.get(0).getValue().length][];
		
		for (int i = 0;i < values.size();i++) {
			resultMatrix[values.get(i).getKey()] = values.get(i).getValue();
		}
		
		return new Matrix(resultMatrix);
	}

}
