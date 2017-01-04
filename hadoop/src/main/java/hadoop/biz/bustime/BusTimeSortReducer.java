package hadoop.biz.bustime;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class BusTimeSortReducer extends
		Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	
	private static int lineNum = 1;
	
	@Override
	public void reduce(LongWritable _key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		context.write(new LongWritable(lineNum ++), _key);
	}

}
