package hadoopstudy.FileCompare;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
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

		Configuration configuration = new PropertiesConfiguration("fileCompare.properties");
		Job job = new Job(new org.apache.hadoop.conf.Configuration(), "dfafaf");
		job.setJarByClass(FileCompare.class);
		job.setJobName("File Comparison");
	    
        job.setReducerClass(FileCompareReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    MultipleInputs.addInputPath(job, new Path(configuration.getString("sourcePath")),TextInputFormat.class,SourceFileMapper.class);
	    MultipleInputs.addInputPath(job, new Path(configuration.getString("destPath")),TextInputFormat.class,DestFileMapper.class);
	    FileOutputFormat.setOutputPath(job, new Path(configuration.getString("jobOutputPath")));
	    System.out.println("sourcePath:"+configuration.getString("sourcePath"));
	    System.out.println("destPath:"+configuration.getString("destPath"));
	    System.out.println("jobOutputPath:"+configuration.getString("jobOutputPath"));
	    
	    System.exit(job.waitForCompletion(true)?0:1);
	}

}
