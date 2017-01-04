package hadoop.biz.bustime;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusTimeSortMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	
	private static final LongWritable ONE = new LongWritable(1);
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String line = ivalue.toString().replaceAll("\"", "");
		String[] args = line.split(",");
		if(ikey.equals(new LongWritable(0))){
			return ;
		}
		
		context.write(new LongWritable(Long.parseLong(args[2])), ONE);
		context.write(new LongWritable(Long.parseLong(args[3])), ONE);
	}

}
