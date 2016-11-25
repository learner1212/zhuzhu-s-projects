package learn.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mapreduce2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("in main method");
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("hdfs://h3/user/odp/compare/compare_result");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path);
        }
        if (fileSystem.exists(new Path("hdfs://h3/user/odp/compare/mapReduceOutput"))) {
            fileSystem.delete(new Path("hdfs://h3/user/odp/compare/mapReduceOutput"));
        }
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path, false);
        fsDataOutputStream.writeBytes("id\tnam1\tnam2\tnam3\tnam4\tnam5\tnam6\tnam7\tnam8\tnam9\tnam10\tdatetime\n");
        Job job = new Job();
        job.setJarByClass(mapreduce2.class);
        job.setJobName("my test");

        FileOutputFormat.setOutputPath(job, new Path("hdfs://h3/user/odp/compare/mapReduceOutput"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FullCompareReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("hdfs://h3/user/odp/compare/sqlpath/part-m-00000"), TextInputFormat.class, sourceMap.class);
        MultipleInputs.addInputPath(job, new Path("hdfs://h3/apps/odp/__hive_db/test_checksum_1/part-m-00000"), TextInputFormat.class, destMap.class);
        if (job.waitForCompletion(true)) {
        }
        fsDataOutputStream.close();
        fileSystem.close();
    }
}