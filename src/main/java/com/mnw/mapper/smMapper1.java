package com.mnw.mapper;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class smMapper1 extends Mapper<LongWritable, Text, Text, WideTableWritable> {
    private Map<String, WideTableWritable> cacheData = new HashMap<>();
    private WideTableWritable outValue = new WideTableWritable();
    private Text outKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //预处理，把要关联的文件加载到缓存中
        URI[] URI = context.getCacheFiles();
        //新的检索缓存文件的API是 context.getCacheFiles() ，而 context.getLocalCacheFiles() 被弃用
        //然而 context.getCacheFiles() 返回的是 HDFS 路径； context.getLocalCacheFiles() 返回的才是本地路径
        FileSystem hdfs = FileSystem.get(context.getConfiguration());

        //这里只缓存了一个文件，所以取第一个即可
        String line = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(URI[0].getPath())), StandardCharsets.UTF_8))) {
            while ((line = reader.readLine()) != null) {
                String[] columnData = StringUtils.split(line, TableInfo.SPLITTER, -1);
                if (StringUtils.equals(columnData[0], TableInfo.T_3RDAPI_SM_RELATION)) {
                    WideTableWritable smLendingWtw = new WideTableWritable();
                    smLendingWtw.setTSmRelation(columnData[1], columnData[3]);
                    cacheData.put(columnData[1], smLendingWtw);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columnData = line.split(TableInfo.SPLITTER, -1);
        switch (columnData[0]) {
            case TableInfo.T_3RDAPI_SM_LOAN:
                //t_3rdapi_sm_loan
                outKey.set(columnData[2]);
                outValue.setTSmLoan(columnData[2], columnData[3], columnData[4], columnData[5], columnData[6], columnData[7], columnData[8], columnData[9], columnData[10], columnData[11], columnData[12]);
                outValue.setTableName(TableInfo.SM_FIRST);
                WideTableWritable smRelationWtw = cacheData.get(columnData[3]);
                outValue.setTSmRelation(smRelationWtw.getSmRelationId(), smRelationWtw.getBehaviorType());
                context.getCounter("mapGet", "smLoan").increment(1);
                break;
            case TableInfo.T_3RDAPI_SM_LENDING:
                //t_3rdapi_sm_lending
                outKey.set(columnData[2]);
                outValue.setTSmLending(columnData[2], columnData[5], columnData[4]);
                outValue.setTableName(TableInfo.T_3RDAPI_SM_LENDING);
                context.getCounter("mapGet", "smLending").increment(1);
                break;
            default:
                outKey.set("N");
                context.getCounter("mapGet", "smNoMatch").increment(1);
                break;

        }
        if (!StringUtils.equals(outKey.toString(), "N") || !StringUtils.equals(outKey.toString(), "\\N")) {
            context.write(outKey, outValue);
        }
    }
}
