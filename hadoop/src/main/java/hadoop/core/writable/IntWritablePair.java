package hadoop.core.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntWritablePair implements WritableComparable<IntWritablePair>{
	
	private IntWritable first;
	
	private IntWritable second;
	
	public IntWritablePair(String first, String second) {
		this(new IntWritable(Integer.parseInt(first)),new IntWritable(Integer.parseInt(second)));
	}
	
	public IntWritablePair(int first, int second) {
		setFirst(new IntWritable(first));
		setSecond(new IntWritable(second));
	}
	
	public IntWritablePair(IntWritable first, IntWritable second) {
		setFirst(first);
		setSecond(second);
	}
	
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public int compareTo(IntWritablePair o) {
		int cmp = first.compareTo(o.getFirst());
		if(cmp != 0) return cmp;
		
		return second.compareTo(o.getSecond());
	}

	public IntWritable getFirst() {
		return first;
	}

	public void setFirst(IntWritable first) {
		this.first = first;
	}

	public IntWritable getSecond() {
		return second;
	}

	public void setSecond(IntWritable second) {
		this.second = second;
	}
	
}
