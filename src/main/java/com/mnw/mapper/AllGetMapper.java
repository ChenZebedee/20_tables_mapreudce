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
 * Created by shaodi.chen on 2018/10/11.
 */
public class AllGetMapper extends Mapper<LongWritable, Text, Text, WideTableWritable> {

    private Text outKey = new Text();
    private WideTableWritable outValue = new WideTableWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columnData = line.split(TableInfo.SPLITTER, -1);

        switch (columnData[0]) {
            case TableInfo.BORROWER_END:
                outValue.textForWritable(columnData);
                if (outValue.getTripartitePrimaryKey().equals("N")){
                    outKey.set("out1"+outValue.getBorrowerBorrowerId());
                }else {
                    outKey.set(outValue.getTripartitePrimaryKey());
                }
                context.getCounter("mapGet", TableInfo.BORROWER_END).increment(1);
                break;
            case TableInfo.BQS_END:
                outValue.textForWritable(columnData);
                outKey.set(outValue.getBqsQueryDataQDId());
                context.getCounter("mapGet", TableInfo.BQS_END).increment(1);

                break;
            case TableInfo.PA_END:
                outValue.textForWritable(columnData);
                outKey.set(outValue.getPaLoanQueryDataQDId());
                context.getCounter("mapGet", TableInfo.PA_END).increment(1);

                break;
            case TableInfo.SM_END:
                outValue.textForWritable(columnData);
                outKey.set(outValue.getSmLoanQDId());
                context.getCounter("mapGet", TableInfo.SM_END).increment(1);

                break;
            case TableInfo.HLSL_END:
                outValue.textForWritable(columnData);
                outKey.set(outValue.getHlslQueryDataQUId());
                context.getCounter("mapGet", TableInfo.HLSL_END).increment(1);
                break;
            case TableInfo.T_3RDAPI_ZXT_HIGHEST_RISK:
                outValue.setTableName(TableInfo.T_3RDAPI_ZXT_HIGHEST_RISK);
                outValue.setTZxtHighestRisk(columnData[2], columnData[9], columnData[10],columnData[5]);
                outKey.set(outValue.getZxtHighestRiskQUId());
                context.getCounter("mapGet", TableInfo.T_3RDAPI_ZXT_HIGHEST_RISK).increment(1);
                break;
            default:
                outKey.set("N");
                context.getCounter("mapGet","noTableNameMatch").increment(1);
                break;
        }
        if (!StringUtils.equals(outKey.toString(), "N") && !StringUtils.equals(outKey.toString(), "\\N")) {
            context.write(outKey, outValue);
            context.getCounter("mapOut","all").increment(1);
        }else{
            context.getCounter("mapGet","badKey").increment(1);
        }

    }
}
