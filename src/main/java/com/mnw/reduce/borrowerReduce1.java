package com.mnw.reduce;

import com.mnw.info.DataUtils;
import com.mnw.info.TableInfo;
import com.mnw.info.WideTableWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class borrowerReduce1 extends Reducer<Text, WideTableWritable, NullWritable, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<WideTableWritable> values, Context context) throws IOException, InterruptedException {
        List<WideTableWritable> borrowerList = new ArrayList<>();
        List<WideTableWritable> borrowerInfoList = new ArrayList<>();
        List<WideTableWritable> borrowerContactList = new ArrayList<>();
        List<WideTableWritable> borrowerExtraList = new ArrayList<>();
        List<WideTableWritable> machineList = new ArrayList<>();
        List<WideTableWritable> qdList = new ArrayList<>();
        for (WideTableWritable wideTableWritable : values) {
            switch (wideTableWritable.getTableName()) {
                case TableInfo.T_RME_BORROWER:
                    WideTableWritable rmeBorrower = new WideTableWritable();
                    rmeBorrower.setTableName(TableInfo.T_RME_BORROWER);
                    rmeBorrower.setTRmeBorrower(wideTableWritable.getBorrowerBorrowerId(), wideTableWritable.getBorrowerOrderSn(), wideTableWritable.getBorrowerLoanType(), wideTableWritable.getBorrowerBorrowerMoney(), wideTableWritable.getBorrowerPayWay(), wideTableWritable.getBorrowerBorrowerPeriod(), wideTableWritable.getBorrowerName(), wideTableWritable.getBorrowerIdCard(), wideTableWritable.getBorrowerBankCard(), wideTableWritable.getBorrowerMobile());
                    borrowerList.add(rmeBorrower);
                    context.getCounter("reduceGet", "rmeBorrower").increment(1);
                    break;
                case TableInfo.T_BORROWER_INFO:
                    WideTableWritable borrowerInfo = new WideTableWritable();
                    borrowerInfo.setTableName(TableInfo.T_BORROWER_INFO);
                    borrowerInfo.setTBorrowerInfo(wideTableWritable.getInfoOrderSn(), wideTableWritable.getBorrowerInfoSex(), wideTableWritable.getBorrowerInfoEducation(), wideTableWritable.getBorrowerInfoMarriageStatus(), wideTableWritable.getBorrowerInfoContactPhone(), wideTableWritable.getBorrowerInfoLoanPurpose(), wideTableWritable.getBorrowerInfoGuaranteeMeasure(), wideTableWritable.getBorrowerInfoIncomeSource(), wideTableWritable.getBorrowerInfoCompanyName(), wideTableWritable.getBorrowerInfoCompanyAddress(), wideTableWritable.getBorrowerInfoCompanyPhone(), wideTableWritable.getBorrowerInfoProfession());
                    borrowerInfoList.add(borrowerInfo);
                    context.getCounter("reduceGet", "borrowerInfo").increment(1);
                    break;
                case TableInfo.T_BORROWER_CONTACT:
                    WideTableWritable borrowerContact = new WideTableWritable();
                    borrowerContact.setTableName(TableInfo.T_BORROWER_CONTACT);
                    borrowerContact.setTBorrowerContact(wideTableWritable.getContactOrderSn(), wideTableWritable.getBorrowerContactEmergencyContactName(), wideTableWritable.getBorrowerContactEmergencyContactRelation(), wideTableWritable.getBorrowerContactEmergencyContactPhone());
                    borrowerContactList.add(borrowerContact);
                    context.getCounter("reduceGet", "borrowerContact").increment(1);
                    break;
                case TableInfo.T_BORROWER_EXTRA:
                    WideTableWritable borrowerExtra = new WideTableWritable();
                    borrowerExtra.setTableName(TableInfo.T_BORROWER_EXTRA);
                    borrowerExtra.setTBorrowerExtraNoInfo(wideTableWritable.getExtraOrderSn(),wideTableWritable.getLoanApplicationTime(),wideTableWritable.getNumberOfMobileLink(),wideTableWritable.getNetTime(),wideTableWritable.getActiveFrequency(),wideTableWritable.getAveCommunicationCost());
                    borrowerExtraList.add(borrowerExtra);
                    context.getCounter("reduceGet", "borrowerExtra").increment(1);
                    break;
                case TableInfo.T_MACHINE_SEARCH_FLOW:
                    WideTableWritable machineWtw = new WideTableWritable();
                    machineWtw.setTableName(TableInfo.T_MACHINE_SEARCH_FLOW);
                    machineWtw.setTMachineSearchFlow(wideTableWritable.getMachineSearchIdentify(),wideTableWritable.getMachineSearchSN());
                    machineList.add(machineWtw);
                    context.getCounter("reduceGet", "machineSearchFlowList").increment(1);
                break;
                case TableInfo.T_3RDAPI_QUERY_DATA:
                    WideTableWritable qdWtw = new WideTableWritable();
                    qdWtw.setTableName(TableInfo.T_3RDAPI_QUERY_DATA);
                    qdWtw.setTQueryData(wideTableWritable.getTripartitePrimaryKey(), wideTableWritable.getQueryDataFlowSn());
                    qdList.add(qdWtw);
                    context.getCounter("reduceGet", "QueryDataList").increment(1);
                break;
                default:
                    context.getCounter("reduceGet", "noMatch").increment(1);
            }
        }

        if (!borrowerList.isEmpty() && !borrowerContactList.isEmpty() && !borrowerInfoList.isEmpty() && !borrowerExtraList.isEmpty()) {
            for (WideTableWritable borrowerWtw : borrowerList) {
                for (WideTableWritable borrowerInfoWtw : borrowerInfoList) {
                    for (WideTableWritable borrowerExtraWtw : borrowerExtraList) {
                        WideTableWritable outWtw = new WideTableWritable();
                        outWtw.setTableName(TableInfo.BORROWER_FIRST);
                        outWtw.setTRmeBorrower(borrowerWtw.getBorrowerBorrowerId(), borrowerWtw.getBorrowerOrderSn(), borrowerWtw.getBorrowerLoanType(), borrowerWtw.getBorrowerBorrowerMoney(), borrowerWtw.getBorrowerPayWay(), borrowerWtw.getBorrowerBorrowerPeriod(), borrowerWtw.getBorrowerName(), borrowerWtw.getBorrowerIdCard(), borrowerWtw.getBorrowerBankCard(), borrowerWtw.getBorrowerMobile());
                        outWtw.setTBorrowerInfo(borrowerInfoWtw.getInfoOrderSn(), borrowerInfoWtw.getBorrowerInfoSex(), borrowerInfoWtw.getBorrowerInfoEducation(), borrowerInfoWtw.getBorrowerInfoMarriageStatus(), borrowerInfoWtw.getBorrowerInfoContactPhone(), borrowerInfoWtw.getBorrowerInfoLoanPurpose(), borrowerInfoWtw.getBorrowerInfoGuaranteeMeasure(), borrowerInfoWtw.getBorrowerInfoIncomeSource(), borrowerInfoWtw.getBorrowerInfoCompanyName(), borrowerInfoWtw.getBorrowerInfoCompanyAddress(), borrowerInfoWtw.getBorrowerInfoCompanyPhone(), borrowerInfoWtw.getBorrowerInfoProfession());
                        outWtw.setTBorrowerExtraNoInfo(borrowerExtraWtw.getExtraOrderSn(),borrowerExtraWtw.getLoanApplicationTime(),borrowerExtraWtw.getNumberOfMobileLink(),borrowerExtraWtw.getNetTime(),borrowerExtraWtw.getActiveFrequency(),borrowerExtraWtw.getAveCommunicationCost());
                            WideTableWritable borrwoerContact = DataUtils.maxOrderSn(borrowerContactList);
                            outWtw.setTBorrowerContact(borrwoerContact.getContactOrderSn(),borrwoerContact.getBorrowerContactEmergencyContactName(),borrwoerContact.getBorrowerContactEmergencyContactRelation(),borrwoerContact.getBorrowerContactEmergencyContactPhone());
                        outWtw.setNumberOfEmergencyContacts("" + borrowerContactList.size());
                        context.getCounter("reduceOut", TableInfo.BORROWER_FIRST).increment(1);
                        outValue.set(outWtw.toStringother());
                        context.write(NullWritable.get(), outValue);
                    }
                }
            }
        }

        if (!machineList.isEmpty()) {
            for (WideTableWritable wideTableWritable : machineList) {
                WideTableWritable outWtw = new WideTableWritable();
                outWtw.setTableName(TableInfo.THIRD_FIRST);
                outWtw.setTMachineSearchFlow(wideTableWritable.getMachineSearchIdentify(), wideTableWritable.getMachineSearchSN());
                if (!qdList.isEmpty()) {
                    for (WideTableWritable wideTableWritable1 : qdList) {
                        outWtw.setTQueryData(wideTableWritable1.getTripartitePrimaryKey(), wideTableWritable1.getQueryDataFlowSn());
                    }
                }
                outValue.set(outWtw.toStringother());
                context.getCounter("reduceOut", TableInfo.THIRD_FIRST).increment(1);
                context.write(NullWritable.get(), outValue);
            }
        }

        borrowerContactList.clear();
        borrowerInfoList.clear();
        borrowerExtraList.clear();
        borrowerList.clear();

    }
}
