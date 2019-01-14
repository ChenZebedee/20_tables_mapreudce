package com.mnw.mapper;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class bqsMapper1 extends Mapper<LongWritable, Text, Text, WideTableWritable> {
    private WideTableWritable outValue = new WideTableWritable();
    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columnData = line.split(TableInfo.SPLITTER, -1);
        switch (columnData[0]) {
            case TableInfo.T_3RDAPI_BQS_STRATEGY:
                //t_3rdapi_bqs_strategy
                outKey.set(columnData[1]);
                outValue.setTBqsStrategy(columnData[2], columnData[1], columnData[3]);
                outValue.setTableName(TableInfo.T_3RDAPI_BQS_STRATEGY);
                context.getCounter("get", "bqsStrategy").increment(1);
                break;
            case TableInfo.T_3RDAPI_BQS_RULE:
                //t_3rdapi_bqs_rule
                outKey.set(columnData[2]);
                outValue.setTBqsRule(columnData[2], columnData[4]);
                outValue.setTableName(TableInfo.T_3RDAPI_BQS_RULE);
                context.getCounter("get", "bqsRule").increment(1);
                break;
            case TableInfo.T_3RDAPI_BQS_QUERY_DATA:
                outKey.set(columnData[1]);
                outValue.setTableName(TableInfo.T_3RDAPI_BQS_QUERY_DATA);
                outValue.setTBqsQueryData(columnData[2], columnData[1], columnData[12],columnData[5]);
                context.getCounter("get", "bqsQd").increment(1);
                break;
            default:
                outKey.set("N");
                context.getCounter("mapGet", "noMatch").increment(1);
                break;
        }
        if (!StringUtils.equals(outKey.toString(), "N") || !StringUtils.equals(outKey.toString(), "\\N")) {
            context.write(outKey, outValue);
        }
    }
}
