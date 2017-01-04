package hadoop.biz.bustime;

import hadoop.core.writable.LongWritablePair;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusTimeMinMaxReducer extends Reducer<Text, LongWritablePair, Text, LongWritablePair> {

	public void reduce(Text _key, Iterable<LongWritablePair> values, Context context)
			throws IOException, InterruptedException {
		
		LongWritable min = new LongWritable(Long.MAX_VALUE);
		LongWritable max = new LongWritable(Long.MIN_VALUE);
		for (LongWritablePair val : values) {
			LongWritable first = val.getFirst();
			if( first.compareTo(min) < 0 ){
				min = first;
			}
			LongWritable second = val.getSecond();
			if( second.compareTo(max) > 0){
				max = second;
			}
		}
		context.write(_key, new LongWritablePair(min, max));
	}

}
