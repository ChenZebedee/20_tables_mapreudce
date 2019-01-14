package com.mnw.mapper;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class bqsMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        WideTableWritable getWtw = new WideTableWritable();
        getWtw.textForWritable(line);
        outValue = value;
        switch (getWtw.getTableName()) {
            case TableInfo.T_3RDAPI_BQS_QUERY_DATA:
                outKey.set(getWtw.getBqsQueryDataId());
                context.getCounter("bqsMapGet", "bqsQd").increment(1);
                break;
            case TableInfo.BQS_FIRST:

                outKey.set(getWtw.getBqsStrategyQDId());
                context.getCounter("bqsMapGet", "bqsFist").increment(1);
                break;
            default:
                outKey.set("N");
                context.getCounter("bqsMapGet", "bqsNoMatch").increment(1);
                break;
        }
        getWtw=null;

        if (!StringUtils.equals(outKey.toString(), "N") || !StringUtils.equals(outKey.toString(), "\\N")) {
            context.write(outKey, outValue);
        }
    }
}
