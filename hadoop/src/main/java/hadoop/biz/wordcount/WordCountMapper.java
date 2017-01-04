package hadoop.biz.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			Text word = new Text();
			word.set(itr.nextToken());
			System.out.println("word:"+word);
			context.write(word, one);
		}
	}
}
