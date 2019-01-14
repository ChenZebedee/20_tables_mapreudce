import com.mnw.info.WideTableWritable;
import com.mnw.mapper.bqsMapper1;
import com.mnw.mapper.bqsMapper2;
import com.mnw.reduce.bqsReduce1;
import com.mnw.reduce.bqsReduce2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

public class bqsMr  {
    private static final Logger LOGGER = LoggerFactory.getLogger(bqsMr.class);

    public static void main(String[] strings) throws InterruptedException, IOException, ClassNotFoundException {
        Date startTime = new Date();
        LOGGER.info(String.valueOf(startTime.getTime()));

        // 1.Get Configuration
        Configuration conf = new Configuration();
        /*conf.set("mapreduce.framework.borrowerName", "local");
        conf.set("fs.defaultFS", "file:///");
        System.setProperty("hadoop.home.dir", "/home/dragon/hadoop-3.1.1");
        System.setProperty("HADOOP_USER_NAME", "hadoop");*/

        //conf.set("inPath1",strings[0]);

        conf.set("inPath1", strings[0]);
        conf.set("inPath2", strings[1]);
        conf.set("inPath3", strings[2]);
        conf.set("WideControlMapreduce.output.fileoutputformat.compress", "false");
        //conf.set("bqsout1", "/home/dragon/bqsout1/");
        conf.set("bqsout1", "/bqsout1/");
        conf.set("bqsOut", strings[3]);
        //conf.set("mapreduce.reduce.memory.mb","8190");
        FileSystem dfs = FileSystem.get(conf);

        // 2.Create Job
        Job job = Job.getInstance(conf, "GetBqsData");
        job.setJarByClass(bqsMr.class);
        //job.setNumReduceTasks(1);
        Path inPath1 = new Path(conf.get("inPath1"));
        FileInputFormat.addInputPath(job, inPath1);
        FileInputFormat.addInputPath(job, new Path(conf.get("inPath2")));
        FileInputFormat.addInputPath(job, new Path(conf.get("inPath3")));
        JobConf mapConf = new JobConf(false);
        ChainMapper.addMapper(job,
                bqsMapper1.class,
                LongWritable.class,
                Text.class,
                Text.class,
                WideTableWritable.class,
                mapConf);
        JobConf reduceConf = new JobConf(false);
        ChainReducer.setReducer(job,
                bqsReduce1.class,
                Text.class,
                WideTableWritable.class,
                NullWritable.class,
                Text.class,
                reduceConf);
        Path outPath1 = new Path(conf.get("bqsout1"));
        if (dfs.exists(outPath1)) {
            dfs.delete(outPath1, true);
        }
        FileOutputFormat.setOutputPath(job, outPath1);


        Job job1 = Job.getInstance(conf, "joinBqsData");
        job1.setJarByClass(bqsMr.class);
//        job1.setNumReduceTasks(11);
        FileInputFormat.addInputPath(job1, new Path(conf.get("bqsout1")));
        JobConf mapConf1 = new JobConf(false);
        ChainMapper.addMapper(job1,
                bqsMapper2.class,
                LongWritable.class,
                Text.class,
                Text.class,
                Text.class,
                mapConf1);
        JobConf reduceConf1 = new JobConf(false);
        ChainReducer.setReducer(job1,
                bqsReduce2.class,
                Text.class,
                Text.class,
                NullWritable.class,
                Text.class,
                reduceConf1);
        Path outPath2 = new Path(conf.get("bqsOut"));
        if (dfs.exists(outPath2)) {
            dfs.delete(outPath2, true);
        }
        FileOutputFormat.setOutputPath(job1, outPath2);

        if (dfs.exists(outPath1)){
            dfs.delete(outPath1,true);
        }

        Date endTime = new Date();
        LOGGER.info(String.valueOf(endTime.getTime()));
        System.exit((job.waitForCompletion(true) && job1.waitForCompletion(true)) ? 0 : 1);
    }


}
