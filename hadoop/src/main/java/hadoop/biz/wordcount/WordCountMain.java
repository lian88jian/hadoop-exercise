
package hadoop.biz.wordcount;
 
import hadoop.core.util.HadoopUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class WordCountMain {
 
 
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		Path inputPath = new Path("hdfs://192.168.138.130:9000/user/hadoop/input");
		Path outputPath = new Path("hdfs://192.168.138.130:9000/user/hadoop/wordCountOutput");
		
		HadoopUtil.delete(conf, outputPath);
		
		Job job = Job.getInstance(conf, "wordCount");
//		job.setNumReduceTasks(2);
		job.setJarByClass(WordCountMain.class);
		job.setMapperClass(WordCountMapper.class);
//		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}