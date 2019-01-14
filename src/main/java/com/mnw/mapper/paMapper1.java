package com.mnw.mapper;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class paMapper1 extends Mapper<LongWritable, Text, Text, WideTableWritable> {
    private WideTableWritable outValue = new WideTableWritable();
    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columnData = line.split(TableInfo.SPLITTER, -1);
        switch (columnData[0]) {
            case TableInfo.T_3RDAPI_PA_LOAN_QUERY_DATA:
                //t_3rdapi_pa_loan_query_data
                outKey.set(columnData[1]);
                outValue.setTPaLoanQueryData(columnData[2], columnData[1],columnData[5]);
                outValue.setTableName(TableInfo.T_3RDAPI_PA_LOAN_QUERY_DATA);
                context.getCounter("获取表的数据量", "paQD").increment(1);
                break;
            case TableInfo.T_3RDAPI_PA_LOAN_RECORD:
                //t_3rdapi_pa_loan_record
                outKey.set(columnData[1]);
                outValue.setTPaLoanRecord(columnData[2], columnData[1]);
                outValue.setTableName(TableInfo.T_3RDAPI_PA_LOAN_RECORD);
                context.getCounter("获取表的数据量", "paRecord").increment(1);
                break;
            case TableInfo.T_3RDAPI_PA_LOAN_CLASSIFICATION:
                //t_3rdapi_pa_loan_classification
                outKey.set(columnData[2]);
                outValue.setTPaLoanClassification(columnData[1], columnData[2], columnData[3], columnData[4], columnData[5]);
                outValue.setTableName(TableInfo.T_3RDAPI_PA_LOAN_CLASSIFICATION);
                context.getCounter("获取表的数据量", "paClass").increment(1);
                break;
            default:
                outKey.set("N");
                context.getCounter("垃圾数据", "毁我青春").increment(1);
                return;
        }
        if (!StringUtils.equals(outKey.toString(), "N") || !StringUtils.equals(outKey.toString(), "\\N")) {
            context.write(outKey, outValue);
        }
    }
}
