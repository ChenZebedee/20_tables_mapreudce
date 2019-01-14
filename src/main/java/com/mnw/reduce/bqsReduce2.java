package com.mnw.reduce;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class bqsReduce2 extends Reducer<Text, Text, NullWritable, Text> {

    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<WideTableWritable> bqsFistList = new ArrayList<>();
        List<WideTableWritable> bqsQdList = new ArrayList<>();


        for (Text value : values) {
            WideTableWritable wideTableWritable = new WideTableWritable();
            wideTableWritable.textForWritable(value.toString());
            if (wideTableWritable.getTableName().equals(TableInfo.T_3RDAPI_BQS_QUERY_DATA)) {
                WideTableWritable reduceGetBqsQd = new WideTableWritable();
                reduceGetBqsQd.setTableName(TableInfo.T_3RDAPI_BQS_QUERY_DATA);
                reduceGetBqsQd.setTBqsQueryData(wideTableWritable.getBqsQueryDataQDId(), wideTableWritable.getBqsQueryDataId(), wideTableWritable.getBqsQueryDataFinalDecision(),wideTableWritable.getBqsQueryDataBorrowerId());
                bqsQdList.add(reduceGetBqsQd);
                context.getCounter("reduceGet", "bqsQd").increment(1);
            } else if (wideTableWritable.getTableName().equals(TableInfo.BQS_FIRST)) {
                WideTableWritable reduceGetBqsFist = new WideTableWritable();
                reduceGetBqsFist.setTableName(TableInfo.BQS_FIRST);
                reduceGetBqsFist.setTBqsStrategy(wideTableWritable.getBqsStrategyQDId(), wideTableWritable.getBqsStrategyId(), wideTableWritable.getStrategyName());
                reduceGetBqsFist.setTBqsRule(wideTableWritable.getBqsRuleStrategyId(), wideTableWritable.getRuleID());
                reduceGetBqsFist.setBqsRuleInfo(wideTableWritable);
                bqsFistList.add(reduceGetBqsFist);
                context.getCounter("reduceGet", TableInfo.BQS_FIRST).increment(1);
            } else {
                context.getCounter("reduceGet", "noMatch").increment(1);
            }

        }


        if (!bqsFistList.isEmpty()) {
            for (WideTableWritable bqsQdWtw : bqsQdList) {
                WideTableWritable outWtw = new WideTableWritable();
                outWtw.setTableName(TableInfo.BQS_END);
                outWtw.setTBqsQueryData(bqsQdWtw.getBqsQueryDataQDId(), bqsQdWtw.getBqsQueryDataId(), bqsQdWtw.getBqsQueryDataFinalDecision(),bqsQdWtw.getBqsQueryDataBorrowerId());
                if (!bqsQdList.isEmpty()) {
                    for (WideTableWritable bqsFistWtw : bqsFistList) {
                        outWtw.setTBqsStrategy(bqsFistWtw.getBqsStrategyQDId(),bqsFistWtw.getBqsStrategyId(),bqsFistWtw.getStrategyName());
                        outWtw.setTBqsRule(bqsFistWtw.getBqsRuleStrategyId(),bqsFistWtw.getRuleID());
                        outWtw.setBqsRuleInfo(bqsFistWtw);

                    }
                }
                outValue.set(outWtw.toStringother());
                context.getCounter("reduceOut",TableInfo.BQS_END).increment(1);
                context.write(NullWritable.get(),outValue);

            }
        }

        bqsFistList.clear();
        bqsQdList.clear();
    }
}
