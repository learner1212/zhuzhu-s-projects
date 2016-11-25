package hadoopstudy.FileCompare;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 


public class FileCompare {
	public static void main(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.println("Usage: MaxTemperature <input path> <output path>");
	        System.exit(-1);
	    }
		
		Job job = new Job();
		job.setJarByClass(FileCompare.class);
		job.setJobName("File Comparison");
	    
	    MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,SourceFileMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,DestFileMapper.class);
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    System.exit(job.waitForCompletion(true)?0:1);
	}

}
