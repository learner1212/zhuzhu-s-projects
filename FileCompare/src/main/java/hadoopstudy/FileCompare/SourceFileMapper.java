package hadoopstudy.FileCompare;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class SourceFileMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		
		context.write(new Text(value), new IntWritable(1));
		 
	 }
}
