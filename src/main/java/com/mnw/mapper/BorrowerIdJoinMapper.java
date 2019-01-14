package com.mnw.mapper;

import com.mnw.info.TableInfo;
import com.mnw.info.TpBorrower;
import com.mnw.info.WideTableWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author shaodi.chen
 * @date 2018/10/15
 */


public class BorrowerIdJoinMapper extends Mapper<LongWritable, Text, Text, WideTableWritable> {

    private Text outKey = new Text();
    private WideTableWritable outValue = new WideTableWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columnData = line.split(TableInfo.SPLITTER, -1);


        switch (columnData[0]) {
            case TableInfo.BORROWER_END:
                outValue.textForWritable(columnData);
                if (outValue.getBorrowerBorrowerId().equals("N")){
                    if (outValue.getTripartitePrimaryKey().equals("N")){
                        outKey.set("n");
                        context.getCounter("mapGet","allNoMatch").increment(1);
                    }else {
                        outKey.set("out2" + outValue.getTripartitePrimaryKey());
                    }
                }else {
                    outKey.set(outValue.getBorrowerBorrowerId());
                }
                context.getCounter("mapGet", TableInfo.BORROWER_END).increment(1);
                break;
            case TableInfo.BQS_END:
                outValue.textForWritable(columnData);
                outKey.set(outValue.getBqsQueryDataBorrowerId());
                context.getCounter("mapGet", TableInfo.BQS_END).increment(1);

                break;
            case TableInfo.PA_END:
                outValue.textForWritable(columnData);
                outKey.set(outValue.getPaLoanQueryDataBorrowerId());
                context.getCounter("mapGet", TableInfo.PA_END).increment(1);
                break;
            case TableInfo.HLSL_END:
                outValue.textForWritable(columnData);
                outKey.set(outValue.getHlslQueryDataBorrowerId());
                context.getCounter("mapGet", TableInfo.HLSL_END).increment(1);
                break;
            case TableInfo.T_3RDAPI_ZXT_HIGHEST_RISK:
                outValue.setTableName(TableInfo.T_3RDAPI_ZXT_HIGHEST_RISK);
                outValue.setTZxtHighestRisk(columnData[2], columnData[9], columnData[10], columnData[5]);
                outKey.set(outValue.getZxtHighestRiskBorrowerId());
                context.getCounter("mapGet", TableInfo.T_3RDAPI_ZXT_HIGHEST_RISK).increment(1);
                break;

            default:
                outKey.set("N");
                context.getCounter("mapGet", "noMatchTableName").increment(1);
                break;
        }
        if (!StringUtils.equals(outKey.toString(), "N") && !StringUtils.equals(outKey.toString(), "\\N")) {
            context.write(outKey, outValue);
        }else{
            context.getCounter("mapGet","badKey").increment(1);
        }

    }
}
