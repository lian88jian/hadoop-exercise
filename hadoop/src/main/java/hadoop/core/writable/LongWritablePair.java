package hadoop.core.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class LongWritablePair implements WritableComparable<LongWritablePair>{
	
	private LongWritable first;
	
	private LongWritable second;
	
	public LongWritablePair() {
		this(new LongWritable(),new LongWritable());
	}
	
	public LongWritablePair(String first, String second) {
		this(new LongWritable(Long.parseLong(first)),new LongWritable(Long.parseLong(second)));
	}
	
	public LongWritablePair(int first, int second) {
		setFirst(new LongWritable(first));
		setSecond(new LongWritable(second));
	}
	
	public LongWritablePair(LongWritable first, LongWritable second) {
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

	public int compareTo(LongWritablePair o) {
		int cmp = first.compareTo(o.getFirst());
		if(cmp != 0) return cmp;
		
		return second.compareTo(o.getSecond());
	}
	
	@Override
	public String toString() {
		return first + "\t" + second;
	}
	
	public LongWritable getFirst() {
		return first;
	}

	public void setFirst(LongWritable first) {
		this.first = first;
	}

	public LongWritable getSecond() {
		return second;
	}

	public void setSecond(LongWritable second) {
		this.second = second;
	}
	
}
