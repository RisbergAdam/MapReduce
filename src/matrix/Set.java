package matrix;

public class Set<V1, V2> {
	
	public final V1 v1;
	public final V2 v2;
	
	public Set(V1 v1, V2 v2) {
		this.v1 = v1;
		this.v2 = v2;
	}

	public String toString() {
		return "[" + v1 + ", " + v2 + "]";
	}
}