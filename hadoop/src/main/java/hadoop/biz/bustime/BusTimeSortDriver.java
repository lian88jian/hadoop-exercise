package hadoop.biz.bustime;

import hadoop.core.util.HadoopUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BusTimeSortDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Path inputPath = new Path("hdfs://192.168.3.136:9000/user/hadoop/input");
		Path outputPath = new Path("hdfs://192.168.3.136:9000/user/hadoop/sortOutput");
		
		HadoopUtil.delete(conf, outputPath);
		
		Job job = Job.getInstance(conf, "BusTimeSort");
		job.setJarByClass(BusTimeSortDriver.class);
		job.setMapperClass(BusTimeSortMapper.class);
//		job.setCombinerClass(BusTimeMinMaxReducer.class);
		job.setReducerClass(BusTimeSortReducer.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (!job.waitForCompletion(true))
			return;
	}

}
