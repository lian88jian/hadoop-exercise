package hadoop.buscount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusCountReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Long min = Long.MAX_VALUE;
		Long max = Long.MIN_VALUE;
		for (Text val : values) {
			String[] times = val.toString().split(",");
			if( min >  Long.parseLong(times[0])){
				min = Long.parseLong(times[0]);
			}
			if( max <  Long.parseLong(times[1])){
				max = Long.parseLong(times[1]);
			}
		}
		context.write(_key, new Text("min:" + min + ",max:" + max));
	}

}
