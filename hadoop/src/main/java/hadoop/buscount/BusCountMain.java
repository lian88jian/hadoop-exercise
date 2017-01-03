
package hadoop.buscount;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class BusCountMain {
 
 
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "busCount");
		job.setJarByClass(BusCountMain.class);
		job.setMapperClass(BusCountMapper.class);
		job.setCombinerClass(BusCountReducer.class);
		job.setReducerClass(BusCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.138.130:9000/user/hadoop/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.138.130:9000/user/hadoop/output6"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}