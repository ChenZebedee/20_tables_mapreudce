import com.mnw.info.WideTableWritable;
import com.mnw.mapper.smMapper1;
import com.mnw.reduce.smReduce1;
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
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.URLPBImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Date;

@SuppressWarnings("ALL")
public class smMr extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(smMr.class);

    public static void main(String[] args) {
        Date startTime = new Date();
        LOGGER.info(String.valueOf(startTime.getTime()));
        Configuration configuration = new Configuration();
        /*configuration.set("mapreduce.framework.borrowerName", "local");
        configuration.set("fs.defaultFS", "file:///");
        System.setProperty("hadoop.home.dir", "E:\\workfile\\binary\\hadoop-3.1.1");
        System.setProperty("HADOOP_USER_NAME", "hadoop");*/
        try {
            int status = ToolRunner.run(configuration, new smMr(), args);
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
        conf.set("smOut", strings[3]);
        FileSystem dfs = FileSystem.get(conf);

        // 2.Create Job
        Job job = Job.getInstance(conf, "GetSMData");
        URI cacheFile = new URI("hdfs://192.168.1.83:9000"+conf.get("inPath2"));
        job.addCacheFile(cacheFile);
        job.setJarByClass(getClass());
//        job.setNumReduceTasks(1);
        Path inPath1 = new Path(conf.get("inPath1"));
        FileInputFormat.addInputPath(job, inPath1);
        FileInputFormat.addInputPath(job, new Path(conf.get("inPath3")));
        JobConf mapConf = new JobConf(false);
        ChainMapper.addMapper(job,
                smMapper1.class,
                LongWritable.class,
                Text.class,
                Text.class,
                WideTableWritable.class,
                mapConf);
        JobConf reduceConf = new JobConf(false);
        ChainReducer.setReducer(job,
                smReduce1.class,
                Text.class,
                WideTableWritable.class,
                NullWritable.class,
                Text.class,
                reduceConf);
        Path outPath1 = new Path(conf.get("smOut"));
        if (dfs.exists(outPath1)) {
            dfs.delete(outPath1, true);
        }
        FileOutputFormat.setOutputPath(job, outPath1);




        return job.waitForCompletion(true) ? 0 : 1;
    }


}
