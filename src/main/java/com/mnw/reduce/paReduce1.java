package com.mnw.reduce;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class paReduce1 extends Reducer<Text, WideTableWritable, NullWritable, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<WideTableWritable> values, Context context) throws IOException, InterruptedException {
        List<WideTableWritable> paRecordList = new ArrayList<>();
        List<WideTableWritable> paClassificationList = new ArrayList<>();
        int status = 0;


        for (WideTableWritable wideTableWritable : values) {

            if (StringUtils.equals(wideTableWritable.getTableName(), TableInfo.T_3RDAPI_PA_LOAN_QUERY_DATA)) {
                WideTableWritable paQDWtw = new WideTableWritable();
                paQDWtw.setTableName(TableInfo.T_3RDAPI_PA_LOAN_QUERY_DATA);
                paQDWtw.setTPaLoanQueryData(wideTableWritable.getPaLoanQueryDataQDId(), wideTableWritable.getPaLoanQueryDataId(),wideTableWritable.getPaLoanQueryDataBorrowerId());
                context.getCounter("reducerOut", "paQueryDataList").increment(1);
                outValue.set(paQDWtw.toStringother());
                context.write(NullWritable.get(), outValue);
            }
            if (StringUtils.equals(wideTableWritable.getTableName(), TableInfo.T_3RDAPI_PA_LOAN_RECORD)) {
                WideTableWritable paRecordWTW = new WideTableWritable();
                paRecordWTW.setTableName(TableInfo.T_3RDAPI_PA_LOAN_RECORD);
                paRecordWTW.setTPaLoanRecord(wideTableWritable.getPaLoanLoanRecordQDId(),wideTableWritable.getPaLoanLoanRecordId());
                paRecordList.add(paRecordWTW);
                context.getCounter("reduceGet", "paRecordList").increment(1);

            }
            if (StringUtils.equals(wideTableWritable.getTableName(), TableInfo.T_3RDAPI_PA_LOAN_CLASSIFICATION)) {
                WideTableWritable paClassificationWTW = new WideTableWritable();
                paClassificationWTW.setTableName(TableInfo.T_3RDAPI_PA_LOAN_CLASSIFICATION);
                paClassificationWTW.setTPaLoanClassification(wideTableWritable.getPaLoanClassificationId(),wideTableWritable.getPaLoanClassificationRId(),wideTableWritable.getPaLoanClassificationClassificationType(),wideTableWritable.getPaLoanClassificationClassificationSection(),wideTableWritable.getPaLoanClassificationOrgNums());
                paClassificationList.add(paClassificationWTW);
                context.getCounter("reduceGet", "paClassificationList").increment(1);
            }
        }





        if (!paRecordList.isEmpty()) {
            for (WideTableWritable wideTableWritable : paRecordList) {
                WideTableWritable outWtw = new WideTableWritable();
                outWtw.setTableName(TableInfo.PA_FIRST);
                outWtw.setTPaLoanRecord(wideTableWritable.getPaLoanLoanRecordQDId(),wideTableWritable.getPaLoanLoanRecordId());
                if (!paClassificationList.isEmpty()) {
                    for (WideTableWritable paClassWtw : paClassificationList) {
                        outWtw.setTPaLoanClassification(paClassWtw.getPaLoanClassificationId(),paClassWtw.getPaLoanClassificationRId(),paClassWtw.getPaLoanClassificationClassificationType(),paClassWtw.getPaLoanClassificationClassificationSection(),paClassWtw.getPaLoanClassificationOrgNums());
                    }
                }
                outValue.set(outWtw.toStringother());
                context.getCounter("reduceOut", TableInfo.PA_FIRST).increment(1);
                context.write(NullWritable.get(), outValue);
            }
        }


        paClassificationList.clear();
        paRecordList.clear();

    }
}
