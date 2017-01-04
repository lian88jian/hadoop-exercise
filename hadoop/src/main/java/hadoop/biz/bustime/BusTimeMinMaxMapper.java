package hadoop.biz.bustime;

import hadoop.core.writable.LongWritablePair;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusTimeMinMaxMapper extends Mapper<LongWritable, Text, Text, LongWritablePair> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String line = ivalue.toString().replaceAll("\"", "");
		String[] args = line.split(",");
		if(ikey.equals(new LongWritable(0))){
			return ;
		}
//		
		context.write(new Text(args[6]), new LongWritablePair(args[2], args[3]));
	}

}
