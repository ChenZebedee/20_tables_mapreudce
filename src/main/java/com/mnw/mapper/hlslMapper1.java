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
 * @class: hlslMapper1
 * @create: 2018-10-13 12:13
 **/


public class hlslMapper1 extends Mapper<LongWritable, Text, Text, WideTableWritable> {

    private Text outKey = new Text();
    private WideTableWritable outValue = new WideTableWritable();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columnData = line.split(TableInfo.SPLITTER, -1);
        switch (columnData[0]) {
            case TableInfo.T_3RDAPI_HLSR_QUERY_DATA:
                outKey.set(columnData[1]);
                outValue.setTableName(TableInfo.T_3RDAPI_HLSR_QUERY_DATA);
                outValue.setTHlslQueryData(columnData[1], columnData[2], columnData[5], columnData[7], columnData[8], columnData[9]);
                context.getCounter("mapGet", TableInfo.T_3RDAPI_HLSR_QUERY_DATA).increment(1);
                break;
            case TableInfo.T_3RDAPI_HLSR_HISTORY_ORG:
                outKey.set(columnData[2]);
                outValue.setTableName(TableInfo.T_3RDAPI_HLSR_HISTORY_ORG);
                outValue.setTHlslHistoryOrg(columnData[2], columnData[5], columnData[8], columnData[4], columnData[7], columnData[3], columnData[9], columnData[6]);
                context.getCounter("mapGet", TableInfo.T_3RDAPI_HLSR_HISTORY_ORG).increment(1);
                break;
            case TableInfo.T_3RDAPI_HLSR_HISTORY_SEARCH:
                outKey.set(columnData[2]);
                outValue.setTableName(TableInfo.T_3RDAPI_HLSR_HISTORY_SEARCH);
                outValue.setTHlslHistorySearch(columnData[2], columnData[10], columnData[5], columnData[9], columnData[6], columnData[7], columnData[4], columnData[8], columnData[3], columnData[12], columnData[16], columnData[13], columnData[14], columnData[11], columnData[15]);
                context.getCounter("mapGet", TableInfo.T_3RDAPI_HLSR_HISTORY_SEARCH).increment(1);
                break;
            case TableInfo.T_3RDAPI_HLSR_USER_BASIC:
                outKey.set(columnData[2]);
                outValue.setTableName(TableInfo.T_3RDAPI_HLSR_USER_BASIC);
                outValue.setTHlslUserBasic(columnData[2], columnData[3], columnData[4], columnData[5], columnData[6], columnData[7], columnData[8], columnData[9], columnData[10], columnData[11], columnData[12], columnData[13], columnData[14], columnData[15], columnData[16], columnData[17], columnData[18]);
                context.getCounter("mapGet", TableInfo.T_3RDAPI_HLSR_USER_BASIC).increment(1);
                break;
            default:
                outKey.set("N");
                context.getCounter("mapGet", "noMatchTableName").increment(1);
                break;
        }

        if (!StringUtils.equals(outKey.toString(), "N") && !StringUtils.equals(outKey.toString(), "\\N")) {
            context.write(outKey, outValue);
        } else {
            context.getCounter("mapGet", "badKey").increment(1);
        }

    }
}
