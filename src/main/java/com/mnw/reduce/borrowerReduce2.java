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

/**
 * @program: riskControl
 * @author: dragon
 * @class: borrowerReduce2
 * @create: 2018-10-10 22:46
 **/


public class borrowerReduce2 extends Reducer<Text, WideTableWritable, NullWritable, Text> {


    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<WideTableWritable> values, Context context) throws IOException, InterruptedException {

        List<WideTableWritable> borrowerList = new ArrayList<>();
        List<WideTableWritable> thirdList = new ArrayList<>();

        for (WideTableWritable wideTableWritable : values) {
            switch (wideTableWritable.getTableName()) {
                case TableInfo.THIRD_FIRST:
                    WideTableWritable thirdWtw = new WideTableWritable();
                    thirdWtw.setTMachineSearchFlow(wideTableWritable.getMachineSearchIdentify(), wideTableWritable.getMachineSearchSN());
                    thirdWtw.setTQueryData(wideTableWritable.getTripartitePrimaryKey(), wideTableWritable.getQueryDataFlowSn());
                    thirdList.add(thirdWtw);
                    context.getCounter("reduceGet", TableInfo.THIRD_FIRST).increment(1);
                    break;
                case TableInfo.BORROWER_FIRST:
                    WideTableWritable borrowerWtw = new WideTableWritable();
                    borrowerWtw.setTRmeBorrower(wideTableWritable.getBorrowerBorrowerId(), wideTableWritable.getBorrowerOrderSn(), wideTableWritable.getBorrowerLoanType(), wideTableWritable.getBorrowerBorrowerMoney(), wideTableWritable.getBorrowerPayWay(), wideTableWritable.getBorrowerBorrowerPeriod(), wideTableWritable.getBorrowerName(), wideTableWritable.getBorrowerIdCard(), wideTableWritable.getBorrowerBankCard(), wideTableWritable.getBorrowerMobile());
                    borrowerWtw.setTBorrowerInfo(wideTableWritable.getInfoOrderSn(), wideTableWritable.getBorrowerInfoSex(), wideTableWritable.getBorrowerInfoEducation(), wideTableWritable.getBorrowerInfoMarriageStatus(), wideTableWritable.getBorrowerInfoContactPhone(), wideTableWritable.getBorrowerInfoLoanPurpose(), wideTableWritable.getBorrowerInfoGuaranteeMeasure(), wideTableWritable.getBorrowerInfoIncomeSource(), wideTableWritable.getBorrowerInfoCompanyName(), wideTableWritable.getBorrowerInfoCompanyAddress(), wideTableWritable.getBorrowerInfoCompanyPhone(), wideTableWritable.getBorrowerInfoProfession());
                    borrowerWtw.setTBorrowerExtraNoInfo(wideTableWritable.getExtraOrderSn(),wideTableWritable.getLoanApplicationTime(),wideTableWritable.getNumberOfMobileLink(),wideTableWritable.getNetTime(),wideTableWritable.getActiveFrequency(),wideTableWritable.getAveCommunicationCost());
                    borrowerWtw.setTBorrowerContact(wideTableWritable.getContactOrderSn(),wideTableWritable.getBorrowerContactEmergencyContactName(),wideTableWritable.getBorrowerContactEmergencyContactRelation(),wideTableWritable.getBorrowerContactEmergencyContactPhone());
                    borrowerWtw.setNumberOfEmergencyContacts(wideTableWritable.getNumberOfEmergencyContacts());

                    borrowerList.add(borrowerWtw);
                    context.getCounter("reduceGet", TableInfo.BORROWER_FIRST).increment(1);
                    break;
                default:
                    context.getCounter("reduceGet", "noMatch").increment(1);
                    break;
            }
        }


        if(!borrowerList.isEmpty()){
            for (WideTableWritable borrowerWtw : borrowerList){
                WideTableWritable outWtw = new WideTableWritable();
                outWtw.setTableName(TableInfo.BORROWER_END);
                outWtw.setTRmeBorrower(borrowerWtw.getBorrowerBorrowerId(),borrowerWtw.getBorrowerOrderSn(),borrowerWtw.getBorrowerLoanType(),borrowerWtw.getBorrowerBorrowerMoney(),borrowerWtw.getBorrowerPayWay(),borrowerWtw.getBorrowerBorrowerPeriod(),borrowerWtw.getBorrowerName(),borrowerWtw.getBorrowerIdCard(),borrowerWtw.getBorrowerBankCard(),borrowerWtw.getBorrowerMobile());
                outWtw.setTBorrowerInfo(borrowerWtw.getInfoOrderSn(),borrowerWtw.getBorrowerInfoSex(),borrowerWtw.getBorrowerInfoEducation(),borrowerWtw.getBorrowerInfoMarriageStatus(),borrowerWtw.getBorrowerInfoContactPhone(),borrowerWtw.getBorrowerInfoLoanPurpose(),borrowerWtw.getBorrowerInfoGuaranteeMeasure(),borrowerWtw.getBorrowerInfoIncomeSource(),borrowerWtw.getBorrowerInfoCompanyName(),borrowerWtw.getBorrowerInfoCompanyAddress(),borrowerWtw.getBorrowerInfoCompanyPhone(),borrowerWtw.getBorrowerInfoProfession());
                outWtw.setTBorrowerExtraNoInfo(borrowerWtw.getExtraOrderSn(),borrowerWtw.getLoanApplicationTime(),borrowerWtw.getNumberOfMobileLink(),borrowerWtw.getNetTime(),borrowerWtw.getActiveFrequency(),borrowerWtw.getAveCommunicationCost());
                outWtw.setTBorrowerContact(borrowerWtw.getContactOrderSn(),borrowerWtw.getBorrowerContactEmergencyContactName(),borrowerWtw.getBorrowerContactEmergencyContactRelation(),borrowerWtw.getBorrowerContactEmergencyContactPhone());
                outWtw.setNumberOfEmergencyContacts(borrowerWtw.getNumberOfEmergencyContacts());
                if (!thirdList.isEmpty()) {
                    WideTableWritable thirdWtw = DataUtils.maxSearchSN(thirdList);
                    outWtw.setTMachineSearchFlow(thirdWtw.getMachineSearchIdentify(), thirdWtw.getMachineSearchSN());
                    outWtw.setTQueryData(thirdWtw.getTripartitePrimaryKey(), thirdWtw.getQueryDataFlowSn());
                }
                outValue.set(outWtw.toStringother());
                context.getCounter("reduceOut",TableInfo.BORROWER_END).increment(1);
                context.write(NullWritable.get(),outValue);
            }
        }

        borrowerList.clear();
        thirdList.clear();


    }
}
