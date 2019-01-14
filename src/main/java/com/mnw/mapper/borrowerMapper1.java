package com.mnw.mapper;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class borrowerMapper1 extends Mapper<LongWritable, Text, Text, WideTableWritable> {
    private WideTableWritable outValue = new WideTableWritable();
    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columnData = line.split(TableInfo.SPLITTER, -1);
        switch (columnData[0]) {
            case TableInfo.T_MACHINE_SEARCH_FLOW:
                //t_machine_search_flow
                outKey.set(columnData[3]);
                outValue.setTMachineSearchFlow(columnData[2], columnData[3]);
                outValue.setTableName(TableInfo.T_MACHINE_SEARCH_FLOW);
                context.getCounter("mapGet", "machine").increment(1);
                break;
            case TableInfo.T_3RDAPI_QUERY_DATA:
                //t_3rdapi_query_data
                outKey.set(columnData[12]);
                outValue.setTQueryData(columnData[1], columnData[12]);
                outValue.setTableName(TableInfo.T_3RDAPI_QUERY_DATA);
                context.getCounter("mapGet", "queryData").increment(1);
                break;
            case TableInfo.T_RME_BORROWER:
                //t_rme_borrower
                outKey.set(columnData[7]);
                outValue.setTRmeBorrower(columnData[2], columnData[7], columnData[8], columnData[22], columnData[21], columnData[23], columnData[16], columnData[17], columnData[20], columnData[19]);
                outValue.setTableName(TableInfo.T_RME_BORROWER);
                context.getCounter("mapGet", "borrower").increment(1);
                break;
            case TableInfo.T_BORROWER_INFO:
                //t_borrower_info
                outKey.set(columnData[2]);
                outValue.setTBorrowerInfo(columnData[2], columnData[9], columnData[10], columnData[11], columnData[12], columnData[14], columnData[30], columnData[21], columnData[15], columnData[16], columnData[17], columnData[18]);
                outValue.setTableName(TableInfo.T_BORROWER_INFO);
                context.getCounter("mapGet", "borrowerInfo").increment(1);
                break;
            case TableInfo.T_BORROWER_CONTACT:
                //t_borrower_contact
                outKey.set(columnData[3]);
                outValue.setTBorrowerContact(columnData[3], columnData[4], columnData[5], columnData[6]);
                outValue.setTableName(TableInfo.T_BORROWER_CONTACT);
                context.getCounter("mapGet", "borrowerContact").increment(1);
                break;
            case TableInfo.T_BORROWER_EXTRA:
                //t_borrower_extra
                outKey.set(columnData[3]);
                outValue.setTBorrowerExtra(columnData[3], columnData[4]);
                outValue.setTableName(TableInfo.T_BORROWER_EXTRA);
                context.getCounter("mapGet", "borrowerExtra").increment(1);
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
