package com.mnw.reduce;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class paReduce2 extends Reducer<Text, WideTableWritable, NullWritable, Text> {

    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<WideTableWritable> values, Context context) throws IOException, InterruptedException {
        List<WideTableWritable> thirdList = new ArrayList<>();
        List<WideTableWritable> paFirstList = new ArrayList<>();
        List<WideTableWritable> paQdList = new ArrayList<>();

        for (WideTableWritable wideTableWritable : values) {
            switch (wideTableWritable.getTableName()) {
                case TableInfo.PA_FIRST:
                    WideTableWritable getPaWtw = new WideTableWritable();
                    getPaWtw.setTableName(TableInfo.PA_FIRST);
                    getPaWtw.setTPaLoanRecord(wideTableWritable.getPaLoanLoanRecordQDId(),wideTableWritable.getPaLoanLoanRecordId());
                    getPaWtw.setTPaLoanClassification(wideTableWritable.getPaLoanClassificationId(),wideTableWritable.getPaLoanClassificationRId(),wideTableWritable.getPaLoanClassificationClassificationType(),wideTableWritable.getPaLoanClassificationClassificationSection(),wideTableWritable.getPaLoanClassificationOrgNums());
                    getPaWtw.setPaClassInfo(wideTableWritable);
                    paFirstList.add(getPaWtw);
                    context.getCounter("reduceGet", TableInfo.PA_FIRST).increment(1);
                    break;
                case TableInfo.T_3RDAPI_PA_LOAN_QUERY_DATA:
                    WideTableWritable getPaQD = new WideTableWritable();
                    getPaQD.setTableName(TableInfo.T_3RDAPI_PA_LOAN_QUERY_DATA);
                    getPaQD.setTPaLoanQueryData(wideTableWritable.getPaLoanQueryDataQDId(), wideTableWritable.getPaLoanQueryDataId(),wideTableWritable.getPaLoanQueryDataBorrowerId());
                    paQdList.add(getPaQD);
                    break;
                default:
                    break;

            }
        }


        if (!paQdList.isEmpty() && !paFirstList.isEmpty()) {
            for (WideTableWritable wideTableWritable : paQdList) {
                WideTableWritable outWtw = new WideTableWritable();
                outWtw.setTableName(TableInfo.PA_END);
                outWtw.setTPaLoanQueryData(wideTableWritable.getPaLoanQueryDataQDId(), wideTableWritable.getPaLoanQueryDataId(),wideTableWritable.getPaLoanQueryDataBorrowerId());
                for (WideTableWritable paFirstWtw : paFirstList) {
                    outWtw.setTPaLoanRecord(paFirstWtw.getPaLoanLoanRecordQDId(),paFirstWtw.getPaLoanLoanRecordId());
                    outWtw.setTPaLoanClassification(paFirstWtw.getPaLoanClassificationId(),paFirstWtw.getPaLoanClassificationRId(),paFirstWtw.getPaLoanClassificationClassificationType(),paFirstWtw.getPaLoanClassificationClassificationSection(),paFirstWtw.getPaLoanClassificationOrgNums());
                    outWtw.setPaClassInfo(paFirstWtw);
                }
                outValue.set(outWtw.toStringother());
                context.getCounter("reduceOut", TableInfo.PA_END).increment(1);
                context.write(NullWritable.get(), outValue);
            }
        }


        paFirstList.clear();
        paQdList.clear();
    }
}

