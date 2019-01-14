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


public class bqsReduce1 extends Reducer<Text, WideTableWritable, NullWritable, Text> {
    private Text outValue = new Text();


    /**
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void reduce(Text key, Iterable<WideTableWritable> values, Context context) throws IOException, InterruptedException {
        List<WideTableWritable> bqsStrategyList = new ArrayList<>();
        List<WideTableWritable> bqsRuleList = new ArrayList<>();
        for (WideTableWritable wideTableWritable : values) {
            if (StringUtils.equals(wideTableWritable.getTableName(), TableInfo.T_3RDAPI_BQS_RULE)) {
                WideTableWritable bqsRuleWtw = new WideTableWritable();
                bqsRuleWtw.setTableName(TableInfo.T_3RDAPI_BQS_RULE);
                bqsRuleWtw.setTBqsRule(wideTableWritable.getBqsRuleStrategyId(), wideTableWritable.getRuleID());
                bqsRuleList.add(bqsRuleWtw);
            } else if (StringUtils.equals(wideTableWritable.getTableName(), TableInfo.T_3RDAPI_BQS_STRATEGY)) {
                WideTableWritable bqsStrategyWtw = new WideTableWritable();
                bqsStrategyWtw.setTBqsStrategy(wideTableWritable.getBqsStrategyQDId(), wideTableWritable.getBqsStrategyId(), wideTableWritable.getStrategyName());
                bqsStrategyList.add(bqsStrategyWtw);
            } else if (StringUtils.equals(wideTableWritable.getTableName(), TableInfo.T_3RDAPI_BQS_QUERY_DATA)) {
                outValue.set(wideTableWritable.toStringother());
                context.getCounter("out", "bqsQD").increment(1);
                context.write(NullWritable.get(), outValue);
            }
        }

        if (!bqsRuleList.isEmpty() && !bqsStrategyList.isEmpty()) {
            for (WideTableWritable bqsStrategyWtw : bqsStrategyList) {
                WideTableWritable outWtw = new WideTableWritable();
                outWtw.setTableName(TableInfo.BQS_FIRST);
                outWtw.setTBqsStrategy(bqsStrategyWtw.getBqsStrategyQDId(), bqsStrategyWtw.getBqsStrategyId(), bqsStrategyWtw.getStrategyName());
                for (WideTableWritable bqsRuleWtw : bqsRuleList) {
                    outWtw.setTBqsRule(bqsRuleWtw.getBqsRuleStrategyId(),bqsRuleWtw.getRuleID());
                }
                outValue.set(outWtw.toStringother());
                context.getCounter("reduceGet", TableInfo.BQS_FIRST).increment(1);
                context.write(NullWritable.get(), outValue);
            }
        }


        bqsRuleList.clear();
        bqsStrategyList.clear();
    }
}
