package com.mnw.reduce;

import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: riskControl
 * @author: dragon
 * @class: hlslReduce1
 * @create: 2018-10-13 12:14
 **/


public class hlslReduce1 extends Reducer<Text, WideTableWritable, NullWritable, Text> {

    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<WideTableWritable> values, Context context) throws IOException, InterruptedException {
        List<WideTableWritable> hlslQdList = new ArrayList<>();
        List<WideTableWritable> hlslHistoryOrgList = new ArrayList<>();
        List<WideTableWritable> hlslHistorySearchList = new ArrayList<>();
        List<WideTableWritable> hlslUserBasicList = new ArrayList<>();


        for (WideTableWritable reduceGet : values) {
            switch (reduceGet.getTableName()) {
                case TableInfo.T_3RDAPI_HLSR_QUERY_DATA:
                    WideTableWritable hlslQdWtw = new WideTableWritable();
                    hlslQdWtw.textForWritable(reduceGet.toStringother());
                    hlslQdList.add(hlslQdWtw);
                    context.getCounter("reduceGet", TableInfo.T_3RDAPI_HLSR_QUERY_DATA).increment(1);
                    break;
                case TableInfo.T_3RDAPI_HLSR_USER_BASIC:
                    WideTableWritable hlslUBWtw = new WideTableWritable();
                    hlslUBWtw.textForWritable(reduceGet.toStringother());
                    hlslUserBasicList.add(hlslUBWtw);
                    context.getCounter("reduceGet", TableInfo.T_3RDAPI_HLSR_USER_BASIC).increment(1);
                    break;
                case TableInfo.T_3RDAPI_HLSR_HISTORY_ORG:
                    WideTableWritable hlslHistoryOrgWtw = new WideTableWritable();
                    hlslHistoryOrgWtw.textForWritable(reduceGet.toStringother());
                    hlslHistoryOrgList.add(hlslHistoryOrgWtw);
                    context.getCounter("reduceGet", TableInfo.T_3RDAPI_HLSR_HISTORY_ORG).increment(1);
                    break;
                case TableInfo.T_3RDAPI_HLSR_HISTORY_SEARCH:
                    WideTableWritable hlslHistorySearchWtw = new WideTableWritable();
                    hlslHistorySearchWtw.textForWritable(reduceGet.toStringother());
                    hlslHistorySearchList.add(hlslHistorySearchWtw);
                    context.getCounter("reduceGet", TableInfo.T_3RDAPI_HLSR_HISTORY_SEARCH).increment(1);
                    break;
                default:
                    context.getCounter("reduceGet", "noMatchTableName").increment(1);
                    break;
            }
        }



        if (!hlslQdList.isEmpty() && ! hlslHistoryOrgList.isEmpty() && !hlslHistorySearchList.isEmpty() && !hlslUserBasicList.isEmpty()){
            for (WideTableWritable qdWtw : hlslQdList){
                WideTableWritable outWtw = new WideTableWritable();
                outWtw.setTableName(TableInfo.HLSL_END);
                outWtw.setTHlslQueryData(qdWtw.getHlslQueryDataId(),qdWtw.getHlslQueryDataQUId(),qdWtw.getHlslQueryDataBorrowerId(),qdWtw.getHlslQueryDataUserName(),qdWtw.getHlslQueryDataUserIdCard(),qdWtw.getHlslQueryDataUserPhone());
                for (WideTableWritable OrgWtw : hlslHistoryOrgList){
                    outWtw.setTHlslHistoryOrg(OrgWtw.getHlslHistoryOrgQUId(),OrgWtw.getHlslHistoryOrgCreditCardRepaymentCnt(),OrgWtw.getHlslHistoryOrgOfflineCashLoanCnt(),OrgWtw.getHlslHistoryOrgOfflineInstallmentCnt(),OrgWtw.getHlslHistoryOrgOnlineCashLoanCnt(),OrgWtw.getHlslHistoryOrgOnlineInstallmentCnt(),OrgWtw.getHlslHistoryOrgOthersCnt(),OrgWtw.getHlslHistoryOrgPaydayLoanCnt());
                }
                for (WideTableWritable searchWtw : hlslHistorySearchList){
                    outWtw.setTHlslHistorySearch(searchWtw.getHlslHistorySearchQUId(),searchWtw.getHlsrHistorySearchOrgCnt(),searchWtw.getHlslHistorySearchSearchCntRecent14Days(),searchWtw.getHlslHistorySearchSearchCntRecent180Days(),searchWtw.getHlslHistorySearchSearchCntRecent30Days(),searchWtw.getHlslHistorySearchSearchCntRecent60Days(),searchWtw.getHlslHistorySearchSearchCntRecent7Days(),searchWtw.getHlslHistorySearchSearchCntRecent90Days(),searchWtw.getHlslHistorySearchSearchCnt(),searchWtw.getHlslHistorySearchOrgCntRecent14Days(),searchWtw.getHlslHistorySearchOrgCntRecent180Days(),searchWtw.getHlslHistorySearchOrgCntRecent30Days(),searchWtw.getHlslHistorySearchOrgCntRecent60Days(),searchWtw.getHlslHistorySearchOrgCntRecent7Days(),searchWtw.getHlslHistorySearchOrgCntRecent90Days());
                }
                for (WideTableWritable userBasicWtw : hlslUserBasicList){
                    outWtw.setTHlslUserBasic(userBasicWtw.getHlslUserBasicQUID(),userBasicWtw.getHlslUserBasicAge(),userBasicWtw.getHlslUserBasicBirthday(),userBasicWtw.getHlslUserBasicGender(),userBasicWtw.getHlslUserBasicIdCardCity(),userBasicWtw.getHlslUserBasicIdCardProvince(),userBasicWtw.getHlslUserBasicIdCardRegion(),userBasicWtw.getHlslUserBasicIdCardValidate(),userBasicWtw.getHlslUserBasicLastAppearIdcard(),userBasicWtw.getHlslUserBasicLastAppearPhone(),userBasicWtw.getHlslUserBasicPhoneCity(),userBasicWtw.getHlslUserBasicPhoneOperator(),userBasicWtw.getHlslUserBasicPhoneProvince(),userBasicWtw.getHlslUserBasicRecordIdCardDays(),userBasicWtw.getHlslUserBasicRecordPhoneDays(),userBasicWtw.getHlslUserBasicUsedIdCardsCnt(),userBasicWtw.getHlslUserBasicUsedPhonesCnt());
                }

                outValue.set(outWtw.toStringother());
                context.getCounter("reduceOut",TableInfo.HLSL_END).increment(1);
                context.write(NullWritable.get(),outValue);
            }
        }


    }
}
