package hadoop.core.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public final class HadoopUtil {
	
  public static List<Text> pivots = new ArrayList<Text>();
  
  private HadoopUtil() { }

  public static void delete(Configuration conf, Path... paths) throws IOException {
    if (conf == null) {
      conf = new Configuration();
    }
    for (Path path : paths) {
      FileSystem fs = path.getFileSystem(conf);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
    }
  }
  
  public static void readPartition(Configuration conf, Path partition) throws IOException{
      FileSystem fs= FileSystem.get(conf);
      FSDataInputStream fin = fs.open(partition);
      BufferedReader in = null;

		String line;
		try {
			in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
			
			while ((line = in.readLine()) != null) {
				pivots.add(new Text(line));
			}			
		
		} finally {
			if (in != null) {
				in.close();
			}
			
		}

  }
  
  public static int getReducerId(Text value, int numPartitions){
	  int result = 0;
	  
	  if (pivots.isEmpty())
			return result;

		if (value.compareTo(pivots.get(0)) < 0){
			result = 0;
		}
		else if (value.compareTo(pivots.get(pivots.size()-1)) > 0){
			result = (pivots.size()-1) % numPartitions;
		}
		else{			
			for(int i=1; i<pivots.size()-1; i++ ){
				if (value.compareTo(pivots.get(i)) >= 0 && value.compareTo(pivots.get(i+1)) < 0){
					result = i % numPartitions;
				}
			}
		}
	  
		return result;
  }
}

