compile:
	javac -d bin/ src/mapreduce/*.java src/mapreduce/worker/*.java src/matrix/*.java src/matrix/operators/*.java

benchmark: compile
	java -classpath bin matrix.MatrixBenchmark 1 1
