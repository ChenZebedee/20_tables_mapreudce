import com.mnw.info.WideTableWritable;
import com.mnw.mapper.BorrowerIdJoinMapper;
import com.mnw.reduce.BorrowerIdJoinReduce;
import org.apache.hadoop.conf.Configuration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 *
 * @author shaodi.chen
 * @date 2018/10/15
 */


public class BorrowerIdJoinMr {


    private static final Logger LOGGER = LoggerFactory.getLogger(BorrowerIdJoinMr.class);


    public static void main(String[] strings) throws IOException, ClassNotFoundException, InterruptedException {
        Date startTime = new Date();
        LOGGER.info(String.valueOf(startTime.getTime()));

        // 1.Get Configuration
        Configuration conf = new Configuration();
        /*conf.set("mapreduce.framework.borrowerName", "local");
        conf.set("fs.defaultFS", "file:///");
        System.setProperty("hadoop.home.dir", "E:\\workfile\\binary\\hadoop-3.1.1");
        System.setProperty("HADOOP_USER_NAME", "hadoop");*/

        //conf.set("inPath1",strings[0]);

        conf.set("inPath1", strings[0]);
        conf.set("inPath2", strings[1]);
        conf.set("inPath3", strings[2]);
        conf.set("inPath4", strings[3]);
        conf.set("inPath5", strings[4]);
        conf.set("WideControlMapreduce.output.fileoutputformat.compress", "false");
//        conf.set("allOut", strings[1]);
        conf.set("joinOut", strings[5]);
        FileSystem dfs = FileSystem.get(conf);

        Job job = Job.getInstance(conf, "GetBqsData");
        job.setJarByClass(BorrowerIdJoinMr.class);
        Path inPath1 = new Path(conf.get("inPath1"));
        FileInputFormat.addInputPath(job, inPath1);
        FileInputFormat.addInputPath(job, new Path(conf.get("inPath2")));
        FileInputFormat.addInputPath(job, new Path(conf.get("inPath3")));
        FileInputFormat.addInputPath(job, new Path(conf.get("inPath4")));
        FileInputFormat.addInputPath(job, new Path(conf.get("inPath5")));
        JobConf mapConf = new JobConf(false);
        ChainMapper.addMapper(job,
                BorrowerIdJoinMapper.class,
                LongWritable.class,
                Text.class,
                Text.class,
                WideTableWritable.class,
                mapConf);
        JobConf reduceConf = new JobConf(false);
        ChainReducer.setReducer(job,
                BorrowerIdJoinReduce.class,
                Text.class,
                WideTableWritable.class,
                NullWritable.class,
                Text.class,
                reduceConf);
        Path outPath1 = new Path(conf.get("joinOut"));
        if (dfs.exists(outPath1)) {
            dfs.delete(outPath1, true);
        }
        FileOutputFormat.setOutputPath(job, outPath1);


        Date endTime = new Date();
        LOGGER.info(String.valueOf(endTime.getTime()));
        System.exit((job.waitForCompletion(true)) ? 0 : 1);
    }
}
