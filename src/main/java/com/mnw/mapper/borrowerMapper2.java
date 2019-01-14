package com.mnw.mapper;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: riskControl
 * @author: dragon
 * @class: borrowerMapper2
 * @create: 2018-10-10 22:45
 **/


public class borrowerMapper2 extends Mapper<LongWritable, Text, Text, WideTableWritable> {

    private Text outKey = new Text();
    private WideTableWritable outValue = new WideTableWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        WideTableWritable getWtw = new WideTableWritable();
        getWtw.textForWritable(value.toString());
        switch (getWtw.getTableName()) {
            case TableInfo.THIRD_FIRST:
                outKey.set(getWtw.getMachineSearchIdentify());
                outValue = getWtw;
                context.getCounter("mapGet", TableInfo.THIRD_FIRST).increment(1);
                break;
            case TableInfo.BORROWER_FIRST:
                outKey.set(getWtw.getBorrowerBorrowerId());
                outValue = getWtw;
                context.getCounter("mapGet", TableInfo.BORROWER_FIRST).increment(1);
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
