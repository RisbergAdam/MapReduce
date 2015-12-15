package matrix;

public class MatrixDemo {

    public static void main(String [] arg) {
        //initialize the MapReduce framework
        MatrixReduce matrixReduce = new MatrixReduce(1, 1);
        
        //create two matrices
        Double [][] matrixValues = {{1.0, 2.0, 0.0}, {2.0, 3.0, 3.0}, {-1.0, 1.0, 2.0}};
        
        Matrix matrix = new Matrix(matrixValues);
        
        //perform some operations using the MapReduce framework
        Matrix matrixInv = matrixReduce.inverse(matrix);
        Matrix maybeUnit = matrixReduce.multiply(matrixInv, matrix);
        
        //print results of operations
        System.out.println("matrix:\n" + matrix.toString());
        System.out.println("matrixInv:\n" + matrixInv.toString());
        System.out.println("matrixInv * matrix:\n" + maybeUnit.toString());
        
        //this is needed to 
        matrixReduce.dispose();
    }
    
}
