package com.mnw.reduce;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class smReduce1 extends Reducer<Text, WideTableWritable, NullWritable, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<WideTableWritable> values, Context context) throws IOException, InterruptedException {
        List<WideTableWritable> smFistList = new ArrayList<>();
        List<WideTableWritable> smLendingList = new ArrayList<>();
        for (WideTableWritable wideTableWritable : values) {
            if (wideTableWritable.getTableName().equals(TableInfo.T_3RDAPI_SM_LENDING)) {
                WideTableWritable smLendingGet = new WideTableWritable();
                smLendingGet.setTSmLending(wideTableWritable.getSmLendingQDId(), wideTableWritable.getOverallRiskLevel(), wideTableWritable.getRiskScore());
                smLendingList.add(smLendingGet);
                context.getCounter("reduceGet", "smLending").increment(1);
            }
            if (wideTableWritable.getTableName().equals(TableInfo.SM_FIRST)) {
                WideTableWritable smFistGet = new WideTableWritable();
                smFistGet.setTSmLoan(wideTableWritable.getSmLoanQDId(), wideTableWritable.getSmLoanLoanAction(), wideTableWritable.getSmLoanPlatformType(), wideTableWritable.getSmLoanVariable(), wideTableWritable.getD3(), wideTableWritable.getD7(), wideTableWritable.getD30(), wideTableWritable.getD60(), wideTableWritable.getD90(), wideTableWritable.getD180(), wideTableWritable.getTotal());
                smFistGet.setTSmRelation(wideTableWritable.getSmRelationId(), wideTableWritable.getBehaviorType());
                smFistList.add(smFistGet);
                context.getCounter("reduceGet", TableInfo.SM_FIRST).increment(1);
            }
        }

        if (!smLendingList.isEmpty() && !smFistList.isEmpty()) {
            for (WideTableWritable smLendingWtw : smLendingList) {
                WideTableWritable outWtw = new WideTableWritable();
                outWtw.setTableName(TableInfo.SM_END);
                outWtw.setTSmLending(smLendingWtw.getSmLendingQDId(), smLendingWtw.getOverallRiskLevel(), smLendingWtw.getRiskScore());
                for (WideTableWritable smFistWtw : smFistList) {
                    outWtw.setTSmLoan(smFistWtw.getSmLoanQDId(), smFistWtw.getSmLoanLoanAction(), smFistWtw.getSmLoanPlatformType(), smFistWtw.getSmLoanVariable(), smFistWtw.getD3(), smFistWtw.getD7(), smFistWtw.getD30(), smFistWtw.getD60(), smFistWtw.getD90(), smFistWtw.getD180(), smFistWtw.getTotal());
                    outWtw.setTSmRelation(smFistWtw.getSmRelationId(), smFistWtw.getBehaviorType());
                }
                outValue.set(outWtw.toStringother());
                context.getCounter("reduceOut", TableInfo.SM_END).increment(1);
                context.write(NullWritable.get(), outValue);
            }
        }


        smFistList.clear();
        smLendingList.clear();

    }
}

