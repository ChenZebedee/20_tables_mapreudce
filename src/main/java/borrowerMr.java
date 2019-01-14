import com.mnw.info.WideTableWritable;
import com.mnw.mapper.borrowerMapper1;
import com.mnw.mapper.borrowerMapper2;
import com.mnw.reduce.borrowerReduce1;
import com.mnw.reduce.borrowerReduce2;
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

public class borrowerMr {
    private static final Logger LOGGER = LoggerFactory.getLogger(borrowerMr.class);

    public static void main(String[] strings) {
        Date startTime = new Date();
        LOGGER.info(String.valueOf(startTime.getTime()));
        Configuration conf = new Configuration();
        /*configuration.set("mapreduce.framework.borrowerName", "local");
        configuration.set("fs.defaultFS", "file:///");
        System.setProperty("hadoop.home.dir", "E:\\workfile\\binary\\hadoop-3.1.1");
        System.setProperty("HADOOP_USER_NAME", "hadoop");*/
        try {

            conf.set("inPath1", strings[0]);
            conf.set("inPath2", strings[1]);
            conf.set("inPath3", strings[2]);
            conf.set("inPath4", strings[3]);
            conf.set("inPath5", strings[4]);
            conf.set("inPath6", strings[5]);
            conf.set("WideControlMapreduce.output.fileoutputformat.compress", "false");
            conf.set("borrowerout1", "/borrowerout1/");
            conf.set("borrowerOut", strings[6]);
            //conf.set("borrowerOut", strings[1]);

            // 2.Create Job
            Job job = Job.getInstance(conf, "GetBORROWERData");
            job.setJarByClass(borrowerMr.class);
//        job.setNumReduceTasks(1);
            Path inPath1 = new Path(conf.get("inPath1"));
            FileInputFormat.addInputPath(job, inPath1);
            FileInputFormat.addInputPath(job, new Path(conf.get("inPath2")));
            FileInputFormat.addInputPath(job, new Path(conf.get("inPath3")));
            FileInputFormat.addInputPath(job, new Path(conf.get("inPath4")));
            FileInputFormat.addInputPath(job, new Path(conf.get("inPath5")));
            FileInputFormat.addInputPath(job, new Path(conf.get("inPath6")));
            JobConf mapConf = new JobConf(false);
            ChainMapper.addMapper(job,
                    borrowerMapper1.class,
                    LongWritable.class,
                    Text.class,
                    Text.class,
                    WideTableWritable.class,
                    mapConf);
            JobConf reduceConf = new JobConf(false);
            ChainReducer.setReducer(job,
                    borrowerReduce1.class,
                    Text.class,
                    WideTableWritable.class,
                    NullWritable.class,
                    Text.class,
                    reduceConf);
            Path outPath1 = new Path(conf.get("borrowerout1"));
            FileSystem dfs = FileSystem.get(conf);
            if (dfs.exists(outPath1)) {
                dfs.delete(outPath1, true);
            }
            FileOutputFormat.setOutputPath(job, outPath1);


            Job job1 = Job.getInstance(conf, "joinBorrowerData");
            job1.setJarByClass(borrowerMr.class);
//        job1.setNumReduceTasks(11);
            FileInputFormat.addInputPath(job1, new Path(conf.get("borrowerout1")));
            JobConf mapConf1 = new JobConf(false);
            ChainMapper.addMapper(job1,
                    borrowerMapper2.class,
                    LongWritable.class,
                    Text.class,
                    Text.class,
                    WideTableWritable.class,
                    mapConf1);
            JobConf reduceConf1 = new JobConf(false);
            ChainReducer.setReducer(job1,
                    borrowerReduce2.class,
                    Text.class,
                    WideTableWritable.class,
                    NullWritable.class,
                    Text.class,
                    reduceConf1);
            Path outPath2 = new Path(conf.get("borrowerOut"));
            if (dfs.exists(outPath2)) {
                dfs.delete(outPath2, true);
            }
            FileOutputFormat.setOutputPath(job1, outPath2);


        /*if (dfs.exists(outPath1)){
            dfs.delete(outPath1,true);
        }*/

            Date endTime = new Date();
            LOGGER.info(String.valueOf(endTime.getTime()));
            System.exit((job.waitForCompletion(true) && job1.waitForCompletion(true)) ? 0 : 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


}
