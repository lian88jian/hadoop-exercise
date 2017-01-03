package hadoop.buscount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String line = ivalue.toString().replaceAll("\"", "");
		String[] args = line.split(",");
		context.write(new Text(args[6]), new Text(args[2] + "," + args[3]));
	}

}
