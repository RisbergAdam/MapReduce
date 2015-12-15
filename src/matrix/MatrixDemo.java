package matrix;

public class MatrixDemo {

    public static void main(String [] arg) {
        //initialize the MapReduce framework
        MatrixReduce matrixReduce = new MatrixReduce(2, 2);
        
        //create two matrices
        Double [][] matrix1Values = {{1.0, 2.0, 0.0}, {2.0, 3.0, 3.0}, {-1.0, 1.0, 2.0}};
        Double [][] matrix2Values = {{0.0, 1.0, -1.0}, {1.0, 0.0, 2.0}, {3.0, -3.0, 3.0}};
        
        Matrix matrix1 = new Matrix(matrix1Values);
        Matrix matrix2 = new Matrix(matrix2Values);
        
        //perform some operations using the MapReduce framework
        Matrix matrix3 = matrixReduce.multiply(matrix1, matrix2);
        Double determinant = matrixReduce.determinant(matrix3);
        Matrix matrix3Inv = matrixReduce.inverse(matrix3);
        Matrix maybeUnit = matrixReduce.multiply(matrix3Inv, matrix3);
        
        //print results of operations
        System.out.println("Determinant of matrix3: " + determinant + "\n");
        System.out.println("matrix3:\n" + matrix3.toString());
        System.out.println("matrix3Inv:\n" + matrix3Inv.toString());
        System.out.println("matrix3Inv * matrix3:\n" + maybeUnit.toString());
        
        //this is needed to 
        matrixReduce.dispose();
    }
    
}
