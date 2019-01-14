package com.mnw.info;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 葫芦索伦
 *
 * @author bo.chao
 */
@Getter
@Setter
@Accessors(chain = true)
public class Hlsl implements Writable {

    //t_3rdapi_hlsr_user_basic
    //f_age
    private String age = "null";

    //f_birthday
    private String birthday = "null";

    //f_gender
    private String gender = "null";

    //f_idcard_city
    private String idCardCity = "null";

    //f_idcard_province
    private String idCardProvince = "null";

    //f_idcard_region
    private String idcardRegion = "null";

    //f_idcard_validate
    private String idcardValidate = "null";

    //f_last_appear_idcard
    private String lastAppearIdcard = "null";

    //f_last_appear_phone
    private String lastAppearPhone = "null";

    //f_phone_city
    private String phoneCity = "null";

    //f_phone_operator
    private String phoneOperator = "null";

    //f_phone_province
    private String phoneProvince = "null";

    //f_record_idcard_days
    private String recordIdcardDays = "null";

    //f_record_phone_days
    private String recordPhoneDays = "null";

    //f_used_idcards_cnt
    private String usedIdcardsCnt = "null";

    //f_used_phones_cnt
    private String usedPhonesCnt = "null";

    //t_3rdapi_hlsr_history_org
    //f_credit_card_repayment_cnt
    private String creditCardRepaymentCnt = "null";

    //f_offline_cash_loan_cnt
    private String offlineCashLoanCnt = "null";

    //f_offline_installment_cnt
    private String offlineInstallmentCnt = "null";

    //f_online_cash_loan_cnt
    private String onlineCashLoanCnt = "null";

    //f_online_installment_cnt
    private String onlineInstallmentCnt = "null";

    //f_others_cnt
    private String othersCnt = "null";

    //f_payday_loan_cnt
    private String paydayLoanCnt = "null";

    //t_3rdapi_hlsr_history_search
    //f_search_cnt
    private String searchCnt = "null";

    //f_search_cnt_recent_7_days
    private String searchCntRecent7Days = "null";

    //f_search_cnt_recent_14_days
    private String searchCntRecent14Days = "null";

    //f_search_cnt_recent_30_days
    private String searchCntRecent30Days = "null";

    //f_search_cnt_recent_60_days
    private String searchCntRecent60Days = "null";

    //f_search_cnt_recent_90_days
    private String searchCntRecent90Days = "null";

    //f_search_cnt_recent_180_days
    private String searchCntRecent180Days = "null";

    //f_org_cnt
    private String orgCnt = "null";

    //f_org_cnt_recent_7_days
    private String orgCntRecent7Days = "null";

    //f_org_cnt_recent_14_days
    private String orgCntRecent14Days = "null";

    //f_org_cnt_recent_30_days
    private String orgCntRecent30Days = "null";

    //f_org_cnt_recent_60_days
    private String orgCntRecent60Days = "null";

    //f_org_cnt_recent_90_days
    private String orgCntRecent90Days = "null";

    //f_org_cnt_recent_180_days
    private String orgCntRecent180Days = "null";

    //用来关联的字段
    //t_3rdapi_hlsr_query_data.id
    private String queryDataId = "null";

    //t_3rdapi_hlsr_user_basic.f_query_data_id
    private String userBasicQueryDataId = "null";

    //t_3rdapi_hlsr_history_org.f_query_data_id
    private String historyOrgQueryDataId = "null";

    //t_3rdapi_hlsr_history_search.f_query_data_id
    private String historySerachQueryDataId = "null";

    public void setHlsl(String age, String birthday, String gender,
                        String idCardCity, String idCardProvince, String idcardRegion, String idcardValidate,
                        String lastAppearIdcard, String lastAppearPhone,
                        String phoneCity, String phoneOperator, String phoneProvince,
                        String recordIdcardDays, String recordPhoneDays, String usedIdcardsCnt, String usedPhonesCnt,
                        String creditCardRepaymentCnt, String offlineCashLoanCnt, String offlineInstallmentCnt,
                        String onlineCashLoanCnt, String onlineInstallmentCnt, String othersCnt, String paydayLoanCnt,
                        String searchCnt,
                        String searchCntRecent7Days,
                        String searchCntRecent14Days,
                        String searchCntRecent30Days,
                        String searchCntRecent60Days,
                        String searchCntRecent90Days,
                        String searchCntRecent180Days,
                        String orgCnt,
                        String orgCntRecent7Days,
                        String orgCntRecent14Days,
                        String orgCntRecent30Days,
                        String orgCntRecent60Days,
                        String orgCntRecent90Days,
                        String orgCntRecent180Days,
                        String queryDataId,
                        String userBasicQueryDataId,
                        String historyOrgQueryDataId,
                        String historySerachQueryDataId) {

        this.age = age;
        this.birthday = birthday;
        this.gender = gender;
        this.idCardCity = idCardCity;
        this.idCardProvince = idCardProvince;
        this.idcardRegion = idcardRegion;
        this.idcardValidate = idcardValidate;
        this.lastAppearIdcard = lastAppearIdcard;
        this.lastAppearPhone = lastAppearPhone;
        this.phoneCity = phoneCity;
        this.phoneOperator = phoneOperator;
        this.phoneProvince = phoneProvince;
        this.recordIdcardDays = recordIdcardDays;
        this.recordPhoneDays = recordPhoneDays;
        this.usedIdcardsCnt = usedIdcardsCnt;
        this.usedPhonesCnt = usedPhonesCnt;
        this.creditCardRepaymentCnt = creditCardRepaymentCnt;
        this.offlineCashLoanCnt = offlineCashLoanCnt;
        this.offlineInstallmentCnt = offlineInstallmentCnt;
        this.onlineCashLoanCnt = onlineCashLoanCnt;
        this.onlineInstallmentCnt = onlineInstallmentCnt;
        this.othersCnt = othersCnt;
        this.paydayLoanCnt = paydayLoanCnt;
        this.searchCnt = searchCnt;
        this.searchCntRecent7Days = searchCntRecent7Days;
        this.searchCntRecent14Days = searchCntRecent14Days;
        this.searchCntRecent30Days = searchCntRecent30Days;
        this.searchCntRecent60Days = searchCntRecent60Days;
        this.searchCntRecent90Days = searchCntRecent90Days;
        this.searchCntRecent180Days = searchCntRecent180Days;
        this.orgCnt = orgCnt;
        this.orgCntRecent7Days = orgCntRecent7Days;
        this.orgCntRecent14Days = orgCntRecent14Days;
        this.orgCntRecent30Days = orgCntRecent30Days;
        this.orgCntRecent60Days = orgCntRecent60Days;
        this.orgCntRecent90Days = orgCntRecent90Days;
        this.orgCntRecent180Days = orgCntRecent180Days;
        this.queryDataId = queryDataId;
        this.userBasicQueryDataId = userBasicQueryDataId;
        this.historyOrgQueryDataId = historyOrgQueryDataId;
        this.historySerachQueryDataId = historySerachQueryDataId;
    }

    /**
     * 设置用户基本信息表信息
     */
    public void setHlsl(String age, String birthday, String gender, String idCardCity, String idCardProvince,
                        String idcardRegion, String idcardValidate, String lastAppearIdcard, String lastAppearPhone,
                        String phoneCity, String phoneOperator, String phoneProvince, String recordIdcardDays,
                        String recordPhoneDays, String usedIdcardsCnt, String usedPhonesCnt) {
        this.age = age;
        this.birthday = birthday;
        this.gender = gender;
        this.idCardCity = idCardCity;
        this.idCardProvince = idCardProvince;
        this.idcardRegion = idcardRegion;
        this.idcardValidate = idcardValidate;
        this.lastAppearIdcard = lastAppearIdcard;
        this.lastAppearPhone = lastAppearPhone;
        this.phoneCity = phoneCity;
        this.phoneOperator = phoneOperator;
        this.phoneProvince = phoneProvince;
        this.recordIdcardDays = recordIdcardDays;
        this.recordPhoneDays = recordPhoneDays;
        this.usedIdcardsCnt = usedIdcardsCnt;
        this.usedPhonesCnt = usedPhonesCnt;
    }

    /**
     * 设置历史机构类型表
     */
    public void setHlsl(String creditCardRepaymentCnt, String offlineCashLoanCnt, String offlineInstallmentCnt,
                        String onlineCashLoanCnt, String onlineInstallmentCnt, String othersCnt, String paydayLoanCnt) {
        this.creditCardRepaymentCnt = creditCardRepaymentCnt;
        this.offlineCashLoanCnt = offlineCashLoanCnt;
        this.offlineInstallmentCnt = offlineInstallmentCnt;
        this.onlineCashLoanCnt = onlineCashLoanCnt;
        this.onlineInstallmentCnt = onlineInstallmentCnt;
        this.othersCnt = othersCnt;
        this.paydayLoanCnt = paydayLoanCnt;
    }

    public void setHlsl(String searchCnt, String searchCntRecent7Days, String searchCntRecent14Days, String searchCntRecent30Days,
                        String searchCntRecent60Days, String searchCntRecent90Days, String searchCntRecent180Days, String orgCnt,
                        String orgCntRecent7Days, String orgCntRecent14Days, String orgCntRecent30Days, String orgCntRecent60Days,
                        String orgCntRecent90Days, String orgCntRecent180Days) {
        this.searchCnt = searchCnt;
        this.searchCntRecent7Days = searchCntRecent7Days;
        this.searchCntRecent14Days = searchCntRecent14Days;
        this.searchCntRecent30Days = searchCntRecent30Days;
        this.searchCntRecent60Days = searchCntRecent60Days;
        this.searchCntRecent90Days = searchCntRecent90Days;
        this.searchCntRecent180Days = searchCntRecent180Days;
        this.orgCnt = orgCnt;
        this.orgCntRecent7Days = orgCntRecent7Days;
        this.orgCntRecent14Days = orgCntRecent14Days;
        this.orgCntRecent30Days = orgCntRecent30Days;
        this.orgCntRecent60Days = orgCntRecent60Days;
        this.orgCntRecent90Days = orgCntRecent90Days;
        this.orgCntRecent180Days = orgCntRecent180Days;
    }

    /**
     * 设置 历史查询表
     */


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.age);
        dataOutput.writeUTF(this.birthday);
        dataOutput.writeUTF(this.gender);
        dataOutput.writeUTF(this.idCardCity);
        dataOutput.writeUTF(this.idCardProvince);
        dataOutput.writeUTF(this.idcardRegion);
        dataOutput.writeUTF(this.idcardValidate);
        dataOutput.writeUTF(this.lastAppearIdcard);
        dataOutput.writeUTF(this.lastAppearPhone);
        dataOutput.writeUTF(this.phoneCity);
        dataOutput.writeUTF(this.phoneOperator);
        dataOutput.writeUTF(this.phoneProvince);
        dataOutput.writeUTF(this.recordIdcardDays);
        dataOutput.writeUTF(this.recordPhoneDays);
        dataOutput.writeUTF(this.usedIdcardsCnt);
        dataOutput.writeUTF(this.usedPhonesCnt);
        dataOutput.writeUTF(this.creditCardRepaymentCnt);
        dataOutput.writeUTF(this.offlineCashLoanCnt);
        dataOutput.writeUTF(this.offlineInstallmentCnt);
        dataOutput.writeUTF(this.onlineCashLoanCnt);
        dataOutput.writeUTF(this.onlineInstallmentCnt);
        dataOutput.writeUTF(this.othersCnt);
        dataOutput.writeUTF(this.paydayLoanCnt);
        dataOutput.writeUTF(this.searchCntRecent14Days);
        dataOutput.writeUTF(this.searchCntRecent180Days);
        dataOutput.writeUTF(this.searchCntRecent30Days);
        dataOutput.writeUTF(this.searchCntRecent60Days);
        dataOutput.writeUTF(this.searchCntRecent7Days);
        dataOutput.writeUTF(this.searchCntRecent90Days);
        dataOutput.writeUTF(this.searchCnt);
        dataOutput.writeUTF(this.orgCnt);
        dataOutput.writeUTF(this.orgCntRecent14Days);
        dataOutput.writeUTF(this.orgCntRecent180Days);
        dataOutput.writeUTF(this.orgCntRecent30Days);
        dataOutput.writeUTF(this.orgCntRecent60Days);
        dataOutput.writeUTF(this.orgCntRecent7Days);
        dataOutput.writeUTF(this.orgCntRecent90Days);
        dataOutput.writeUTF(this.queryDataId);
        dataOutput.writeUTF(this.userBasicQueryDataId);
        dataOutput.writeUTF(this.historyOrgQueryDataId);
        dataOutput.writeUTF(this.historySerachQueryDataId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.age = dataInput.readUTF();
        this.birthday = dataInput.readUTF();
        this.gender = dataInput.readUTF();
        this.idCardCity = dataInput.readUTF();
        this.idCardProvince = dataInput.readUTF();
        this.idcardRegion = dataInput.readUTF();
        this.idcardValidate = dataInput.readUTF();
        this.lastAppearIdcard = dataInput.readUTF();
        this.lastAppearPhone = dataInput.readUTF();
        this.phoneCity = dataInput.readUTF();
        this.phoneOperator = dataInput.readUTF();
        this.phoneProvince = dataInput.readUTF();
        this.recordIdcardDays = dataInput.readUTF();
        this.recordPhoneDays = dataInput.readUTF();
        this.usedIdcardsCnt = dataInput.readUTF();
        this.usedPhonesCnt = dataInput.readUTF();
        this.creditCardRepaymentCnt = dataInput.readUTF();
        this.offlineCashLoanCnt = dataInput.readUTF();
        this.offlineInstallmentCnt = dataInput.readUTF();
        this.onlineCashLoanCnt = dataInput.readUTF();
        this.onlineInstallmentCnt = dataInput.readUTF();
        this.othersCnt = dataInput.readUTF();
        this.paydayLoanCnt = dataInput.readUTF();
        this.searchCntRecent14Days = dataInput.readUTF();
        this.searchCntRecent180Days = dataInput.readUTF();
        this.searchCntRecent30Days = dataInput.readUTF();
        this.searchCntRecent60Days = dataInput.readUTF();
        this.searchCntRecent7Days = dataInput.readUTF();
        this.searchCntRecent90Days = dataInput.readUTF();
        this.searchCnt = dataInput.readUTF();
        this.orgCnt = dataInput.readUTF();
        this.orgCntRecent14Days = dataInput.readUTF();
        this.orgCntRecent180Days = dataInput.readUTF();
        this.orgCntRecent30Days = dataInput.readUTF();
        this.orgCntRecent60Days = dataInput.readUTF();
        this.orgCntRecent7Days = dataInput.readUTF();
        this.orgCntRecent90Days = dataInput.readUTF();
        this.queryDataId = dataInput.readUTF();
        this.userBasicQueryDataId = dataInput.readUTF();
        this.historyOrgQueryDataId = dataInput.readUTF();
        this.historySerachQueryDataId = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Hlsl hlsl = (Hlsl) o;

        if (getAge() != null ? !getAge().equals(hlsl.getAge()) : hlsl.getAge() != null) {
            return false;
        }
        if (getBirthday() != null ? !getBirthday().equals(hlsl.getBirthday()) : hlsl.getBirthday() != null) {
            return false;
        }
        if (getGender() != null ? !getGender().equals(hlsl.getGender()) : hlsl.getGender() != null) {
            return false;
        }
        if (getIdCardCity() != null ? !getIdCardCity().equals(hlsl.getIdCardCity()) : hlsl.getIdCardCity() != null) {
            return false;
        }
        if (getIdCardProvince() != null ? !getIdCardProvince().equals(hlsl.getIdCardProvince()) : hlsl.getIdCardProvince() != null) {
            return false;
        }
        if (getIdcardRegion() != null ? !getIdcardRegion().equals(hlsl.getIdcardRegion()) : hlsl.getIdcardRegion() != null) {
            return false;
        }
        if (getIdcardValidate() != null ? !getIdcardValidate().equals(hlsl.getIdcardValidate()) : hlsl.getIdcardValidate() != null) {
            return false;
        }
        if (getLastAppearIdcard() != null ? !getLastAppearIdcard().equals(hlsl.getLastAppearIdcard()) : hlsl.getLastAppearIdcard() != null) {
            return false;
        }
        if (getLastAppearPhone() != null ? !getLastAppearPhone().equals(hlsl.getLastAppearPhone()) : hlsl.getLastAppearPhone() != null) {
            return false;
        }
        if (getPhoneCity() != null ? !getPhoneCity().equals(hlsl.getPhoneCity()) : hlsl.getPhoneCity() != null) {
            return false;
        }
        if (getPhoneOperator() != null ? !getPhoneOperator().equals(hlsl.getPhoneOperator()) : hlsl.getPhoneOperator() != null) {
            return false;
        }
        if (getPhoneProvince() != null ? !getPhoneProvince().equals(hlsl.getPhoneProvince()) : hlsl.getPhoneProvince() != null) {
            return false;
        }
        if (getRecordIdcardDays() != null ? !getRecordIdcardDays().equals(hlsl.getRecordIdcardDays()) : hlsl.getRecordIdcardDays() != null) {
            return false;
        }
        if (getRecordPhoneDays() != null ? !getRecordPhoneDays().equals(hlsl.getRecordPhoneDays()) : hlsl.getRecordPhoneDays() != null) {
            return false;
        }
        if (getUsedIdcardsCnt() != null ? !getUsedIdcardsCnt().equals(hlsl.getUsedIdcardsCnt()) : hlsl.getUsedIdcardsCnt() != null) {
            return false;
        }
        if (getUsedPhonesCnt() != null ? !getUsedPhonesCnt().equals(hlsl.getUsedPhonesCnt()) : hlsl.getUsedPhonesCnt() != null) {
            return false;
        }
        if (getCreditCardRepaymentCnt() != null ? !getCreditCardRepaymentCnt().equals(hlsl.getCreditCardRepaymentCnt()) : hlsl.getCreditCardRepaymentCnt() != null) {
            return false;
        }
        if (getOfflineCashLoanCnt() != null ? !getOfflineCashLoanCnt().equals(hlsl.getOfflineCashLoanCnt()) : hlsl.getOfflineCashLoanCnt() != null) {
            return false;
        }
        if (getOfflineInstallmentCnt() != null ? !getOfflineInstallmentCnt().equals(hlsl.getOfflineInstallmentCnt()) : hlsl.getOfflineInstallmentCnt() != null) {
            return false;
        }
        if (getOnlineCashLoanCnt() != null ? !getOnlineCashLoanCnt().equals(hlsl.getOnlineCashLoanCnt()) : hlsl.getOnlineCashLoanCnt() != null) {
            return false;
        }
        if (getOnlineInstallmentCnt() != null ? !getOnlineInstallmentCnt().equals(hlsl.getOnlineInstallmentCnt()) : hlsl.getOnlineInstallmentCnt() != null) {
            return false;
        }
        if (getOthersCnt() != null ? !getOthersCnt().equals(hlsl.getOthersCnt()) : hlsl.getOthersCnt() != null) {
            return false;
        }
        if (getPaydayLoanCnt() != null ? !getPaydayLoanCnt().equals(hlsl.getPaydayLoanCnt()) : hlsl.getPaydayLoanCnt() != null) {
            return false;
        }
        if (getSearchCntRecent7Days() != null ? !getSearchCntRecent7Days().equals(hlsl.getSearchCntRecent7Days()) : hlsl.getSearchCntRecent7Days() != null) {
            return false;
        }
        if (getSearchCnt() != null ? !getSearchCnt().equals(hlsl.getSearchCnt()) : hlsl.getSearchCnt() != null) {
            return false;
        }
        if (getSearchCntRecent14Days() != null ? !getSearchCntRecent14Days().equals(hlsl.getSearchCntRecent14Days()) : hlsl.getSearchCntRecent14Days() != null) {
            return false;
        }
        if (getSearchCntRecent30Days() != null ? !getSearchCntRecent30Days().equals(hlsl.getSearchCntRecent30Days()) : hlsl.getSearchCntRecent30Days() != null) {
            return false;
        }
        if (getSearchCntRecent60Days() != null ? !getSearchCntRecent60Days().equals(hlsl.getSearchCntRecent60Days()) : hlsl.getSearchCntRecent60Days() != null) {
            return false;
        }
        if (getSearchCntRecent90Days() != null ? !getSearchCntRecent90Days().equals(hlsl.getSearchCntRecent90Days()) : hlsl.getSearchCntRecent90Days() != null) {
            return false;
        }
        if (getSearchCntRecent180Days() != null ? !getSearchCntRecent180Days().equals(hlsl.getSearchCntRecent180Days()) : hlsl.getSearchCntRecent180Days() != null) {
            return false;
        }
        if (getOrgCnt() != null ? !getOrgCnt().equals(hlsl.getOrgCnt()) : hlsl.getOrgCnt() != null) {
            return false;
        }
        if (getOrgCntRecent7Days() != null ? !getOrgCntRecent7Days().equals(hlsl.getOrgCntRecent7Days()) : hlsl.getOrgCntRecent7Days() != null) {
            return false;
        }
        if (getOrgCntRecent14Days() != null ? !getOrgCntRecent14Days().equals(hlsl.getOrgCntRecent14Days()) : hlsl.getOrgCntRecent14Days() != null) {
            return false;
        }
        if (getOrgCntRecent30Days() != null ? !getOrgCntRecent30Days().equals(hlsl.getOrgCntRecent30Days()) : hlsl.getOrgCntRecent30Days() != null) {
            return false;
        }
        if (getOrgCntRecent60Days() != null ? !getOrgCntRecent60Days().equals(hlsl.getOrgCntRecent60Days()) : hlsl.getOrgCntRecent60Days() != null) {
            return false;
        }
        if (getOrgCntRecent90Days() != null ? !getOrgCntRecent90Days().equals(hlsl.getOrgCntRecent90Days()) : hlsl.getOrgCntRecent90Days() != null) {
            return false;
        }
        if (getOrgCntRecent180Days() != null ? !getOrgCntRecent180Days().equals(hlsl.getOrgCntRecent180Days()) : hlsl.getOrgCntRecent180Days() != null) {
            return false;
        }
        if (getQueryDataId() != null ? !getQueryDataId().equals(hlsl.getQueryDataId()) : hlsl.getQueryDataId() != null) {
            return false;
        }
        if (getUserBasicQueryDataId() != null ? !getUserBasicQueryDataId().equals(hlsl.getUserBasicQueryDataId()) : hlsl.getUserBasicQueryDataId() != null) {
            return false;
        }
        if (getHistoryOrgQueryDataId() != null ? !getHistoryOrgQueryDataId().equals(hlsl.getHistoryOrgQueryDataId()) : hlsl.getHistoryOrgQueryDataId() != null) {
            return false;
        }
        return getHistorySerachQueryDataId() != null ? getHistorySerachQueryDataId().equals(hlsl.getHistorySerachQueryDataId()) : hlsl.getHistorySerachQueryDataId() == null;
    }

    @Override
    public int hashCode() {
        int result = getAge() != null ? getAge().hashCode() : 0;
        result = 31 * result + (getBirthday() != null ? getBirthday().hashCode() : 0);
        result = 31 * result + (getGender() != null ? getGender().hashCode() : 0);
        result = 31 * result + (getIdCardCity() != null ? getIdCardCity().hashCode() : 0);
        result = 31 * result + (getIdCardProvince() != null ? getIdCardProvince().hashCode() : 0);
        result = 31 * result + (getIdcardRegion() != null ? getIdcardRegion().hashCode() : 0);
        result = 31 * result + (getIdcardValidate() != null ? getIdcardValidate().hashCode() : 0);
        result = 31 * result + (getLastAppearIdcard() != null ? getLastAppearIdcard().hashCode() : 0);
        result = 31 * result + (getLastAppearPhone() != null ? getLastAppearPhone().hashCode() : 0);
        result = 31 * result + (getPhoneCity() != null ? getPhoneCity().hashCode() : 0);
        result = 31 * result + (getPhoneOperator() != null ? getPhoneOperator().hashCode() : 0);
        result = 31 * result + (getPhoneProvince() != null ? getPhoneProvince().hashCode() : 0);
        result = 31 * result + (getRecordIdcardDays() != null ? getRecordIdcardDays().hashCode() : 0);
        result = 31 * result + (getRecordPhoneDays() != null ? getRecordPhoneDays().hashCode() : 0);
        result = 31 * result + (getUsedIdcardsCnt() != null ? getUsedIdcardsCnt().hashCode() : 0);
        result = 31 * result + (getUsedPhonesCnt() != null ? getUsedPhonesCnt().hashCode() : 0);
        result = 31 * result + (getCreditCardRepaymentCnt() != null ? getCreditCardRepaymentCnt().hashCode() : 0);
        result = 31 * result + (getOfflineCashLoanCnt() != null ? getOfflineCashLoanCnt().hashCode() : 0);
        result = 31 * result + (getOfflineInstallmentCnt() != null ? getOfflineInstallmentCnt().hashCode() : 0);
        result = 31 * result + (getOnlineCashLoanCnt() != null ? getOnlineCashLoanCnt().hashCode() : 0);
        result = 31 * result + (getOnlineInstallmentCnt() != null ? getOnlineInstallmentCnt().hashCode() : 0);
        result = 31 * result + (getOthersCnt() != null ? getOthersCnt().hashCode() : 0);
        result = 31 * result + (getPaydayLoanCnt() != null ? getPaydayLoanCnt().hashCode() : 0);
        result = 31 * result + (getSearchCnt() != null ? getSearchCnt().hashCode() : 0);
        result = 31 * result + (getSearchCntRecent7Days() != null ? getSearchCntRecent7Days().hashCode() : 0);
        result = 31 * result + (getSearchCntRecent14Days() != null ? getSearchCntRecent14Days().hashCode() : 0);
        result = 31 * result + (getSearchCntRecent30Days() != null ? getSearchCntRecent30Days().hashCode() : 0);
        result = 31 * result + (getSearchCntRecent60Days() != null ? getSearchCntRecent60Days().hashCode() : 0);
        result = 31 * result + (getSearchCntRecent90Days() != null ? getSearchCntRecent90Days().hashCode() : 0);
        result = 31 * result + (getSearchCntRecent180Days() != null ? getSearchCntRecent180Days().hashCode() : 0);
        result = 31 * result + (getOrgCnt() != null ? getOrgCnt().hashCode() : 0);
        result = 31 * result + (getOrgCntRecent7Days() != null ? getOrgCntRecent7Days().hashCode() : 0);
        result = 31 * result + (getOrgCntRecent14Days() != null ? getOrgCntRecent14Days().hashCode() : 0);
        result = 31 * result + (getOrgCntRecent30Days() != null ? getOrgCntRecent30Days().hashCode() : 0);
        result = 31 * result + (getOrgCntRecent60Days() != null ? getOrgCntRecent60Days().hashCode() : 0);
        result = 31 * result + (getOrgCntRecent90Days() != null ? getOrgCntRecent90Days().hashCode() : 0);
        result = 31 * result + (getOrgCntRecent180Days() != null ? getOrgCntRecent180Days().hashCode() : 0);
        result = 31 * result + (getQueryDataId() != null ? getQueryDataId().hashCode() : 0);
        result = 31 * result + (getUserBasicQueryDataId() != null ? getUserBasicQueryDataId().hashCode() : 0);
        result = 31 * result + (getHistoryOrgQueryDataId() != null ? getHistoryOrgQueryDataId().hashCode() : 0);
        result = 31 * result + (getHistorySerachQueryDataId() != null ? getHistorySerachQueryDataId().hashCode() : 0);
        return result;
    }

    public String toOtherString() {
        StringBuffer sb = new StringBuffer();
        sb.append(age).append("$|$")
            .append(birthday).append("$|$")
            .append(gender).append("$|$")
            .append(idCardCity).append("$|$")
            .append(idCardProvince).append("$|$")
            .append(idcardRegion).append("$|$")
            .append(idcardValidate).append("$|$")
            .append(lastAppearIdcard).append("$|$")
            .append(lastAppearPhone).append("$|$")
            .append(phoneCity).append("$|$")
            .append(phoneOperator).append("$|$")
            .append(phoneProvince).append("$|$")
            .append(recordIdcardDays).append("$|$")
            .append(recordPhoneDays).append("$|$")
            .append(usedIdcardsCnt).append("$|$")
            .append(usedPhonesCnt).append("$|$")
            .append(creditCardRepaymentCnt).append("$|$")
            .append(offlineCashLoanCnt).append("$|$")
            .append(offlineInstallmentCnt).append("$|$")
            .append(onlineCashLoanCnt).append("$|$")
            .append(onlineInstallmentCnt).append("$|$")
            .append(othersCnt).append("$|$")
            .append(paydayLoanCnt).append("$|$")
            .append(searchCnt).append("$|$")
            .append(searchCntRecent7Days).append("$|$")
            .append(searchCntRecent14Days).append("$|$")
            .append(searchCntRecent30Days).append("$|$")
            .append(searchCntRecent60Days).append("$|$")
            .append(searchCntRecent90Days).append("$|$")
            .append(searchCntRecent180Days).append("$|$")
            .append(orgCnt).append("$|$")
            .append(orgCntRecent7Days).append("$|$")
            .append(orgCntRecent14Days).append("$|$")
            .append(orgCntRecent30Days).append("$|$")
            .append(orgCntRecent60Days).append("$|$")
            .append(orgCntRecent90Days).append("$|$")
            .append(orgCntRecent180Days).append("$|$")
            .append(queryDataId).append("$|$")
            .append(userBasicQueryDataId).append("$|$")
            .append(historyOrgQueryDataId).append("$|$")
            .append(historySerachQueryDataId);
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(age)
            .append(",").append(birthday)
            .append(",").append(gender)
            .append(",").append(idCardCity)
            .append(",").append(idCardProvince)
            .append(",").append(idcardRegion)
            .append(",").append(idcardValidate)
            .append(",").append(lastAppearIdcard)
            .append(",").append(lastAppearPhone)
            .append(",").append(phoneCity)
            .append(",").append(phoneOperator)
            .append(",").append(phoneProvince)
            .append(",").append(recordIdcardDays)
            .append(",").append(recordPhoneDays)
            .append(",").append(usedIdcardsCnt)
            .append(",").append(usedPhonesCnt)
            .append(",").append(creditCardRepaymentCnt)
            .append(",").append(offlineCashLoanCnt)
            .append(",").append(offlineInstallmentCnt)
            .append(",").append(onlineCashLoanCnt)
            .append(",").append(onlineInstallmentCnt)
            .append(",").append(othersCnt)
            .append(",").append(paydayLoanCnt)
            .append(",").append(searchCnt)
            .append(",").append(searchCntRecent7Days)
            .append(",").append(searchCntRecent14Days)
            .append(",").append(searchCntRecent30Days)
            .append(",").append(searchCntRecent60Days)
            .append(",").append(searchCntRecent90Days)
            .append(",").append(searchCntRecent180Days)
            .append(",").append(orgCnt)
            .append(",").append(orgCntRecent7Days)
            .append(",").append(orgCntRecent14Days)
            .append(",").append(orgCntRecent30Days)
            .append(",").append(orgCntRecent60Days)
            .append(",").append(orgCntRecent90Days)
            .append(",").append(orgCntRecent180Days);
        return sb.toString();
    }
}
