
package hadoop.biz.bustime;
 
import hadoop.core.writable.LongWritablePair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class BusTimeMinMaxDriver {
 
 
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "busCount1");
		job.setJarByClass(BusTimeMinMaxDriver.class);
		job.setMapperClass(BusTimeMinMaxMapper.class);
		job.setCombinerClass(BusTimeMinMaxReducer.class);
		job.setReducerClass(BusTimeMinMaxReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritablePair.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.138.130:9000/user/hadoop/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.138.130:9000/user/hadoop/output5"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}