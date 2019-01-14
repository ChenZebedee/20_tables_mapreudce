import com.mnw.info.WideTableWritable;
import com.mnw.mapper.paMapper1;
import com.mnw.mapper.paMapper2;
import com.mnw.reduce.paReduce1;
import com.mnw.reduce.paReduce2;
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

import java.util.Date;

public class paMr extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(paMr.class);

    public static void main(String[] args) {
        Date startTime = new Date();
        LOGGER.info(String.valueOf(startTime.getTime()));
        Configuration configuration = new Configuration();
        //configuration.set("yarn.resourcemanager.hostname", "data1");
        //configuration.set("mapreduce.framework.borrowerName", "yarn");
        //configuration.set("fs.default.borrowerName", "hdfs://data1:9000");
        //本地调试模式
        /*configuration.set("mapreduce.framework.borrowerName", "local");
        configuration.set("fs.defaultFS", "file:///");
        System.setProperty("hadoop.home.dir", "/home/dragon/hadoop-3.1.1");
        System.setProperty("HADOOP_USER_NAME", "hadoop");*/
        try {
            int status = ToolRunner.run(configuration, new paMr(), args);
            LOGGER.info(String.valueOf(status));
        } catch (Exception e) {
            LOGGER.error("ToolRunner 错误");
            e.printStackTrace();
        }
        Date endTime = new Date();
        LOGGER.info(String.valueOf(endTime.getTime()));
    }

    @Override
    public int run(String[] strings) throws Exception {
        // 1.Get Configuration
        Configuration conf = super.getConf();

//        conf.set("mapreduce.app-submission.cross-platform", "true");//跨平台提交
        //conf.set("mapred.jar","/home/data/dragon/coding/mrtest/target/riskControl-1.0-SNAPSHOT.jar");
        conf.set("inPath1", strings[0]);
        conf.set("inPath2", strings[1]);
        conf.set("inPath3", strings[2]);
        conf.set("WideControlMapreduce.output.fileoutputformat.compress", "false");
        conf.set("paout1", "/paout1/");
        conf.set("paOut", strings[3]);
        FileSystem dfs = FileSystem.get(conf);

        // 2.Create Job
        Job job = Job.getInstance(conf, "GetPAData");
        job.setJarByClass(getClass());
//        job.setNumReduceTasks(1);
        Path inPath1 = new Path(conf.get("inPath1"));
        FileInputFormat.addInputPath(job, inPath1);
        FileInputFormat.addInputPath(job, new Path(conf.get("inPath2")));
        FileInputFormat.addInputPath(job, new Path(conf.get("inPath3")));
        JobConf mapConf = new JobConf(false);
        ChainMapper.addMapper(job,
                paMapper1.class,
                LongWritable.class,
                Text.class,
                Text.class,
                WideTableWritable.class,
                mapConf);
        JobConf reduceConf = new JobConf(false);
        ChainReducer.setReducer(job,
                paReduce1.class,
                Text.class,
                WideTableWritable.class,
                NullWritable.class,
                Text.class,
                reduceConf);
        Path outPath1 = new Path(conf.get("paout1"));
        if (dfs.exists(outPath1)) {
            dfs.delete(outPath1, true);
        }
        FileOutputFormat.setOutputPath(job, outPath1);


        Job job1 = Job.getInstance(conf, "joinPAData");
        job1.setJarByClass(getClass());
//        job1.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job1, new Path(conf.get("paout1")));
        JobConf mapConf1 = new JobConf(false);
        ChainMapper.addMapper(job1,
                paMapper2.class,
                LongWritable.class,
                Text.class,
                Text.class,
                WideTableWritable.class,
                mapConf1);
        JobConf reduceConf1 = new JobConf(false);
        ChainReducer.setReducer(job1,
                paReduce2.class,
                Text.class,
                WideTableWritable.class,
                NullWritable.class,
                Text.class,
                reduceConf1);
        Path outPath2 = new Path(conf.get("paOut"));
        if (dfs.exists(outPath2)) {
            dfs.delete(outPath2, true);
        }
        FileOutputFormat.setOutputPath(job1, outPath2);


        if (dfs.exists(outPath1)){
            dfs.delete(outPath1,true);
        }


        boolean isSuccess = false;

        if (job.waitForCompletion(true)) {
                isSuccess = job1.waitForCompletion(true);
        }


        return isSuccess ? 0 : 1;
    }


}
