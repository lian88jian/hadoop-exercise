package hadoop.core.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>{
	
	private Text first;
	
	private Text second;
	
	public TextPair() {
		this(new Text(),new Text());
	}
	
	public TextPair(String first, String second) {
		setFirst(new Text(first));
		setSecond(new Text(second));
	}
	
	public TextPair(Text first, Text second) {
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

	public int compareTo(TextPair o) {
		int cmp = first.compareTo(o.getFirst());
		if(cmp != 0) return cmp;
		
		return second.compareTo(o.getSecond());
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}
	
}
