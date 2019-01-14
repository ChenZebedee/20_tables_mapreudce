package com.mnw.mapper;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class paMapper2 extends Mapper<LongWritable, Text, Text, WideTableWritable> {
    private Text outKey = new Text();
    private WideTableWritable outValue = new WideTableWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columnData = line.split(TableInfo.SPLITTER, -1);
        WideTableWritable getWtw = new WideTableWritable();
            /*if (columnData.length<=2) {
                return;
            }*/
        getWtw.textForWritable(columnData);

        switch (columnData[0]) {
            case TableInfo.PA_FIRST:
                outKey.set(getWtw.getPaLoanLoanRecordQDId());
                outValue = getWtw;
                context.getCounter("mapGet", TableInfo.PA_FIRST).increment(1);
                break;
            case TableInfo.T_3RDAPI_PA_LOAN_QUERY_DATA:
                outKey.set(getWtw.getPaLoanQueryDataId());
                outValue = getWtw;
                context.getCounter("mapGet", "paQd").increment(1);
                break;
            default:
                outKey.set("N");
                context.getCounter("垃圾数据", "毁我青春").increment(1);
                break;
        }

        if (!StringUtils.equals(outKey.toString(), "N") || !StringUtils.equals(outKey.toString(), "\\N")) {
            context.write(outKey, outValue);
        }

    }
}
