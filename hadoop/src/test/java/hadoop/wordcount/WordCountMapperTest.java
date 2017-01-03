package hadoop.wordcount;

import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class WordCountMapperTest {

	@Test
	public void test() {
		
		Text inValue = new Text("lian jian lian");
		WordCountMapper mapper = new WordCountMapper();
	}

}
