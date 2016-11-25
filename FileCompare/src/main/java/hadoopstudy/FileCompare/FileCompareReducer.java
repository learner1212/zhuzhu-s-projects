package hadoopstudy.FileCompare;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileCompareReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
		Context context) 
		throws IOException, InterruptedException {
		int len = 0;
		int file = 0;
		for(IntWritable value : values)
		{
			len++;
			file = value.get();
		}
		if(len != 2){
			context.write(new Text(key), new IntWritable(file));
		}

	 }
	
	
}
