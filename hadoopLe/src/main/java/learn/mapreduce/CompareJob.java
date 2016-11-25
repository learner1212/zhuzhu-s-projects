package learn.mapreduce;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import learn.mapreduce.path.verifyDataConf;

public class CompareJob {
    private Path sourcePath;
    private Path destPath;
    private Path mapReduceOutputPath;
    private FileSystem fileSystem;
    // private FSDataOutputStream fsDataOutputStream;
    private Configuration conf;
    private org.apache.hadoop.conf.Configuration hadoopConf;
    private Job job;

    public CompareJob(Configuration conf) {
        this.conf = conf;
        prepare();
        init();
    }

    public CompareJob(Configuration conf, Class<? extends Reducer> reduceClass) {
        this.conf = conf;
        prepare();
        init(reduceClass);
    }

    private void prepare() {
        try {
            hadoopConf = new org.apache.hadoop.conf.Configuration();
            fileSystem = FileSystem.get(hadoopConf);
            mapReduceOutputPath = new Path(conf.getString(verifyDataConf.JOBOUPUT));
            if (fileSystem.exists(mapReduceOutputPath)) {
                fileSystem.delete(mapReduceOutputPath);
            }
            // hadoopConf.set("stream.reduce.output.field.separator", "\001");
            hadoopConf.set(verifyDataConf.KEYBITSET, conf.getString(verifyDataConf.KEYBITSET));
            job = new Job(hadoopConf);
            sourcePath = new Path(conf.getString(verifyDataConf.TARGETDIR));
            destPath = new Path(conf.getString(verifyDataConf.HIVEPATH));
            System.out.println("sourcePath:" + sourcePath.toString());
            System.out.println("destPath:" + destPath.toString());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void init(Class<? extends Reducer> reduceClass) {
        // fsDataOutputStream.writeBytes("0;0;" +
        // conf.getString(verifyDataConf.KEYS) + ";" +
        // conf.getString(verifyDataConf.FILENAME) + "\n");
        // fsDataOutputStream.writeBytes("0;0;");
        // reduce.setFsDataOutputStream(fsDataOutputStream);
        job.setJarByClass(CompareJob.class);
        job.setJobName(conf.getString(verifyDataConf.TABLENAME));
        // job.setMapperClass(destMap.class);
        // FileInputFormat.addInputPath(job, destPath);
        FileOutputFormat.setOutputPath(job, mapReduceOutputPath);
        job.setReducerClass(reduceClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, sourcePath, TextInputFormat.class, sourceMap.class);
        MultipleInputs.addInputPath(job, destPath, TextInputFormat.class, destMap.class);
        job.setNumReduceTasks(5);
    }

    private void init() {
        // fsDataOutputStream.writeBytes("0;0;" +
        // conf.getString(verifyDataConf.KEYS) + ";" +
        // conf.getString(verifyDataConf.FILENAME) + "\n");
        // fsDataOutputStream.writeBytes("0;0;");
        // reduce.setFsDataOutputStream(fsDataOutputStream);
        job.setJarByClass(CompareJob.class);
        job.setJobName(conf.getString(verifyDataConf.TABLENAME));
        // job.setMapperClass(destMap.class);
        // FileInputFormat.addInputPath(job, destPath);
        FileOutputFormat.setOutputPath(job, mapReduceOutputPath);
        job.setReducerClass(FullCompareReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, sourcePath, TextInputFormat.class, sourceMap.class);
        MultipleInputs.addInputPath(job, destPath, TextInputFormat.class, destMap.class);
        job.setNumReduceTasks(5);
    }

    public long[] runTask() {
        long result[] = new long[4];
        try {
            if (job.waitForCompletion(true)) {

                result[0] = job.getCounters().findCounter("Count", "sourceCount").getValue();
                result[1] = job.getCounters().findCounter("Count", "destCount").getValue();
                result[2] = job.getCounters().findCounter("Count", "matchCount").getValue();
                result[3] = job.getCounters().findCounter("Count", "diffCount").getValue();
                System.out.println("源记录数：" + result[0]);
                System.out.println("目标记录数：" + result[1]);
                System.out.println("匹配记录数：" + result[2]);
                System.out.println("不匹配记录数：" + result[3]);
            }
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    public void release() {
        try {
            // if (fsDataOutputStream != null) {
            // fsDataOutputStream.close();
            // }
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            // TODO: handle exception
        }
    }

    public static void main(String[] args) {
        System.out.println("\003");
    }

}
