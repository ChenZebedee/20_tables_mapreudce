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
 * Created by shaodi.chen on 2018/10/11.
 */
public class AllGetReduce extends Reducer<Text, WideTableWritable, NullWritable, Text> {

    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<WideTableWritable> values, Context context) throws IOException, InterruptedException {

        List<WideTableWritable> borrowerList = new ArrayList<>();
        List<WideTableWritable> bqsList = new ArrayList<>();
        List<WideTableWritable> paList = new ArrayList<>();
        List<WideTableWritable> smList = new ArrayList<>();
        List<WideTableWritable> hlslList = new ArrayList<>();
        List<WideTableWritable> zxtList = new ArrayList<>();
        for (WideTableWritable wideTableWritable : values) {
            if (key.toString().equals("out1"+wideTableWritable.getBorrowerBorrowerId())){
                context.getCounter("reduceOut","borrowerId").increment(1);
                outValue.set(wideTableWritable.toStringother());
                context.write(NullWritable.get(),outValue);
            }else {
                switch (wideTableWritable.getTableName()) {
                    case TableInfo.BORROWER_END:
                        WideTableWritable borrowerWtw = new WideTableWritable();
                        borrowerWtw.textForWritable(wideTableWritable.toStringother());
                        borrowerList.add(borrowerWtw);
                        context.getCounter("reduceGet", TableInfo.BORROWER_END).increment(1);
                        break;
                    case TableInfo.BQS_END:
                        WideTableWritable bqsWtw = new WideTableWritable();
                        bqsWtw.textForWritable(wideTableWritable.toStringother());
                        bqsList.add(bqsWtw);
                        context.getCounter("reduceGet", TableInfo.BQS_END).increment(1);
                        break;
                    case TableInfo.PA_END:
                        WideTableWritable paWtw = new WideTableWritable();
                        paWtw.textForWritable(wideTableWritable.toStringother());
                        paList.add(paWtw);
                        context.getCounter("reduceGet", TableInfo.PA_END).increment(1);
                        break;
                    case TableInfo.SM_END:
                        WideTableWritable smWtw = new WideTableWritable();
                        smWtw.textForWritable(wideTableWritable.toStringother());
                        smList.add(smWtw);
                        context.getCounter("reduceGet", TableInfo.SM_END).increment(1);
                        break;
                    case TableInfo.HLSL_END:
                        WideTableWritable hlslWtw = new WideTableWritable();
                        hlslWtw.textForWritable(wideTableWritable.toStringother());
                        hlslList.add(hlslWtw);
                        context.getCounter("reduceGet", TableInfo.HLSL_END).increment(1);
                        break;
                    case TableInfo.T_3RDAPI_ZXT_HIGHEST_RISK:
                        WideTableWritable zxtWtw = new WideTableWritable();
                        zxtWtw.textForWritable(wideTableWritable.toStringother());
                        zxtList.add(zxtWtw);
                        context.getCounter("reduceGet", TableInfo.T_3RDAPI_ZXT_HIGHEST_RISK).increment(1);
                        break;
                    default:
                        break;
                }
            }
        }

        if (!borrowerList.isEmpty()){
            for (WideTableWritable borrowerWtw : borrowerList) {
                WideTableWritable outWtw = new WideTableWritable();
                outWtw.setTableName(TableInfo.BORROWER_END);
                outWtw.setTRmeBorrower(borrowerWtw.getBorrowerBorrowerId(), borrowerWtw.getBorrowerOrderSn(), borrowerWtw.getBorrowerLoanType(), borrowerWtw.getBorrowerBorrowerMoney(), borrowerWtw.getBorrowerPayWay(), borrowerWtw.getBorrowerBorrowerPeriod(), borrowerWtw.getBorrowerName(), borrowerWtw.getBorrowerIdCard(), borrowerWtw.getBorrowerBankCard(), borrowerWtw.getBorrowerMobile());
                outWtw.setTBorrowerInfo(borrowerWtw.getInfoOrderSn(),borrowerWtw.getBorrowerInfoSex(),borrowerWtw.getBorrowerInfoEducation(),borrowerWtw.getBorrowerInfoMarriageStatus(),borrowerWtw.getBorrowerInfoContactPhone(),borrowerWtw.getBorrowerInfoLoanPurpose(),borrowerWtw.getBorrowerInfoGuaranteeMeasure(),borrowerWtw.getBorrowerInfoIncomeSource(),borrowerWtw.getBorrowerInfoCompanyName(),borrowerWtw.getBorrowerInfoCompanyAddress(),borrowerWtw.getBorrowerInfoCompanyPhone(),borrowerWtw.getBorrowerInfoProfession());
                outWtw.setTBorrowerExtraNoInfo(borrowerWtw.getExtraOrderSn(), borrowerWtw.getLoanApplicationTime(), borrowerWtw.getNumberOfMobileLink(), borrowerWtw.getNetTime(), borrowerWtw.getActiveFrequency(), borrowerWtw.getAveCommunicationCost());
                outWtw.setTBorrowerContact(borrowerWtw.getContactOrderSn(),borrowerWtw.getBorrowerContactEmergencyContactName(),borrowerWtw.getBorrowerContactEmergencyContactRelation(),borrowerWtw.getBorrowerContactEmergencyContactPhone());
                outWtw.setNumberOfEmergencyContacts(borrowerWtw.getNumberOfEmergencyContacts());
                outWtw.setTMachineSearchFlow(borrowerWtw.getMachineSearchIdentify(),borrowerWtw.getMachineSearchSN());
                outWtw.setTQueryData(borrowerWtw.getTripartitePrimaryKey(), borrowerWtw.getQueryDataFlowSn());


                if (!bqsList.isEmpty()) {
                    for (WideTableWritable bqsWtw : bqsList) {
                        outWtw.setTBqsQueryData(bqsWtw.getBqsQueryDataQDId(), bqsWtw.getBqsQueryDataId(), bqsWtw.getBqsQueryDataFinalDecision(),bqsWtw.getBqsQueryDataBorrowerId());
                        outWtw.setTBqsStrategy(bqsWtw.getBqsStrategyQDId(), bqsWtw.getBqsStrategyId(), bqsWtw.getStrategyName());
                        outWtw.setTBqsRule(bqsWtw.getBqsRuleStrategyId(), bqsWtw.getRuleID());
                        outWtw.setBqsRuleInfo(bqsWtw);
                    }
                }
                if (!paList.isEmpty()) {
                    for (WideTableWritable paWtw : paList) {
                        outWtw.setTPaLoanQueryData(paWtw.getPaLoanQueryDataQDId(), paWtw.getPaLoanQueryDataId(),paWtw.getPaLoanQueryDataBorrowerId());
                        outWtw.setTPaLoanRecord(paWtw.getPaLoanLoanRecordQDId(),paWtw.getPaLoanLoanRecordId());
                        outWtw.setTPaLoanClassification(paWtw.getPaLoanClassificationId(), paWtw.getPaLoanClassificationRId(), paWtw.getPaLoanClassificationClassificationType(), paWtw.getPaLoanClassificationClassificationSection(), paWtw.getPaLoanClassificationOrgNums());
                        outWtw.setPaClassInfo(paWtw);
                    }
                }
                if (!hlslList.isEmpty()) {
                    for (WideTableWritable hlslWtw : hlslList) {
                        outWtw.setTHlslQueryData(hlslWtw.getHlslQueryDataId(), hlslWtw.getPaLoanQueryDataQDId(),hlslWtw.getHlslQueryDataBorrowerId(),hlslWtw.getHlslQueryDataUserName(),hlslWtw.getHlslQueryDataUserIdCard(),hlslWtw.getHlslQueryDataUserPhone());
                        outWtw.setTHlslHistoryOrg(hlslWtw.getHlslHistoryOrgQUId(), hlslWtw.getHlslHistoryOrgCreditCardRepaymentCnt(), hlslWtw.getHlslHistoryOrgOfflineCashLoanCnt(), hlslWtw.getHlslHistoryOrgOfflineInstallmentCnt(), hlslWtw.getHlslHistoryOrgOnlineCashLoanCnt(), hlslWtw.getHlslHistoryOrgOnlineInstallmentCnt(), hlslWtw.getHlslHistoryOrgOthersCnt(), hlslWtw.getHlslHistoryOrgPaydayLoanCnt());
                        outWtw.setTHlslHistorySearch(hlslWtw.getHlslHistorySearchQUId(), hlslWtw.getHlsrHistorySearchOrgCnt(), hlslWtw.getHlslHistorySearchSearchCntRecent14Days(), hlslWtw.getHlslHistorySearchSearchCntRecent180Days(), hlslWtw.getHlslHistorySearchSearchCntRecent30Days(), hlslWtw.getHlslHistorySearchSearchCntRecent60Days(), hlslWtw.getHlslHistorySearchSearchCntRecent7Days(), hlslWtw.getHlslHistorySearchSearchCntRecent90Days(), hlslWtw.getHlslHistorySearchSearchCnt(), hlslWtw.getHlslHistorySearchOrgCntRecent14Days(), hlslWtw.getHlslHistorySearchOrgCntRecent180Days(), hlslWtw.getHlslHistorySearchOrgCntRecent30Days(), hlslWtw.getHlslHistorySearchOrgCntRecent60Days(), hlslWtw.getHlslHistorySearchOrgCntRecent7Days(), hlslWtw.getHlslHistorySearchOrgCntRecent90Days());
                        outWtw.setTHlslUserBasic(hlslWtw.getHlslUserBasicQUID(), hlslWtw.getHlslUserBasicAge(), hlslWtw.getHlslUserBasicBirthday(), hlslWtw.getBorrowerInfoSex(), hlslWtw.getHlslUserBasicIdCardCity(), hlslWtw.getHlslUserBasicIdCardProvince(), hlslWtw.getHlslUserBasicIdCardRegion(), hlslWtw.getHlslUserBasicIdCardValidate(), hlslWtw.getHlslUserBasicLastAppearIdcard(), hlslWtw.getHlslUserBasicLastAppearPhone(), hlslWtw.getHlslUserBasicPhoneCity(), hlslWtw.getHlslUserBasicPhoneOperator(), hlslWtw.getHlslUserBasicPhoneProvince(), hlslWtw.getHlslUserBasicRecordIdCardDays(), hlslWtw.getHlslUserBasicRecordPhoneDays(), hlslWtw.getHlslUserBasicUsedIdCardsCnt(), hlslWtw.getHlslUserBasicUsedPhonesCnt());
                    }
                }
                if (!zxtList.isEmpty()) {
                    for (WideTableWritable zxtWtw : zxtList) {
                        outWtw.setTZxtHighestRisk(zxtWtw.getZxtHighestRiskQUId(), zxtWtw.getHighestRiskLevelDescription(), zxtWtw.getHighestRiskRevel(),zxtWtw.getZxtHighestRiskBorrowerId());
                    }
                }

                if (!smList.isEmpty()) {
                    for (WideTableWritable smWtw : smList) {
                        outWtw.setSmInfo(smWtw);
                    }
                }


                outValue.set(outWtw.toStringother());
                context.getCounter("reduceOut", TableInfo.BORROWER_END).increment(1);
                context.write(NullWritable.get(), outValue);

            }
        }

        borrowerList.clear();
        bqsList.clear();
        paList.clear();
        hlslList.clear();
        smList.clear();
        zxtList.clear();
    }
}
