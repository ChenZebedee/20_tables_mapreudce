package com.mnw.info;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@Getter
@Setter
public class WideTableWritable implements Writable {

    private String tableName = "N";

    //machine
    private String machineSearchIdentify = "N";
    private String machineSearchSN = "N";

    //qd
    private String queryDataFlowSn = "N";
    private String tripartitePrimaryKey = "N";

    //boorower

    private String borrowerBorrowerId = "N";
    private String borrowerOrderSn = "N";
    private String borrowerLoanType = "N";
    private String borrowerBorrowerMoney = "N";
    private String borrowerPayWay = "N";
    private String borrowerBorrowerPeriod = "N";
    private String borrowerName = "N";
    private String borrowerIdCard = "N";
    private String borrowerBankCard = "N";
    private String borrowerMobile = "N";

    private String infoOrderSn = "N";
    private String borrowerInfoSex = "N";
    private String borrowerInfoEducation = "N";
    private String borrowerInfoMarriageStatus = "N";
    private String borrowerInfoContactPhone = "N";
    private String borrowerInfoLoanPurpose = "N";
    private String borrowerInfoGuaranteeMeasure = "N";
    private String borrowerInfoIncomeSource = "N";
    private String borrowerInfoCompanyName = "N";
    private String borrowerInfoCompanyAddress = "N";
    private String borrowerInfoCompanyPhone = "N";
    private String borrowerInfoProfession = "N";

    private String contactOrderSn = "N";
    private String borrowerContactEmergencyContactName = "N";
    private String borrowerContactEmergencyContactRelation = "N";
    private String borrowerContactEmergencyContactPhone = "N";
    private String numberOfEmergencyContacts = "N";

    private String extraOrderSn = "N";
    private String additionalInformationOfPartner = "N";
    private String loanApplicationTime = "N";
    private String numberOfMobileLink = "N";
    private String netTime = "N";
    private String activeFrequency = "N";
    private String aveCommunicationCost = "N";

    //hlsl
    private String hlslQueryDataId = "N";
    private String hlslQueryDataQUId = "N";
    private String hlslQueryDataBorrowerId = "N";
    private String hlslQueryDataUserName = "N";
    private String hlslQueryDataUserIdCard = "N";
    private String hlslQueryDataUserPhone = "N";

    private String hlslUserBasicQUID = "N";
    private String hlslUserBasicAge = "N";
    private String hlslUserBasicBirthday = "N";
    private String hlslUserBasicGender = "N";
    private String hlslUserBasicIdCardCity = "N";
    private String hlslUserBasicIdCardProvince = "N";
    private String hlslUserBasicIdCardRegion = "N";
    private String hlslUserBasicIdCardValidate = "N";
    private String hlslUserBasicLastAppearIdcard = "N";
    private String hlslUserBasicLastAppearPhone = "N";
    private String hlslUserBasicPhoneCity = "N";
    private String hlslUserBasicPhoneOperator = "N";
    private String hlslUserBasicPhoneProvince = "N";
    private String hlslUserBasicRecordIdCardDays = "N";
    private String hlslUserBasicRecordPhoneDays = "N";
    private String hlslUserBasicUsedIdCardsCnt = "N";
    private String hlslUserBasicUsedPhonesCnt = "N";

    private String hlslHistoryOrgQUId = "N";
    private String hlslHistoryOrgCreditCardRepaymentCnt = "N";
    private String hlslHistoryOrgOfflineCashLoanCnt = "N";
    private String hlslHistoryOrgOfflineInstallmentCnt = "N";
    private String hlslHistoryOrgOnlineCashLoanCnt = "N";
    private String hlslHistoryOrgOnlineInstallmentCnt = "N";
    private String hlslHistoryOrgOthersCnt = "N";
    private String hlslHistoryOrgPaydayLoanCnt = "N";

    private String hlslHistorySearchQUId = "N";
    private String hlsrHistorySearchOrgCnt = "N";
    private String hlslHistorySearchSearchCntRecent14Days = "N";
    private String hlslHistorySearchSearchCntRecent180Days = "N";
    private String hlslHistorySearchSearchCntRecent30Days = "N";
    private String hlslHistorySearchSearchCntRecent60Days = "N";
    private String hlslHistorySearchSearchCntRecent7Days = "N";
    private String hlslHistorySearchSearchCntRecent90Days = "N";
    private String hlslHistorySearchSearchCnt = "N";
    private String hlslHistorySearchOrgCntRecent14Days = "N";
    private String hlslHistorySearchOrgCntRecent180Days = "N";
    private String hlslHistorySearchOrgCntRecent30Days = "N";
    private String hlslHistorySearchOrgCntRecent60Days = "N";
    private String hlslHistorySearchOrgCntRecent7Days = "N";
    private String hlslHistorySearchOrgCntRecent90Days = "N";

    //pa
    private String paLoanQueryDataQDId = "N";
    private String paLoanQueryDataId = "N";
    private String paLoanQueryDataBorrowerId = "N";

    private String paLoanLoanRecordQDId = "N";
    private String paLoanLoanRecordId = "N";

    private String paLoanClassificationId = "N";
    private String paLoanClassificationRId = "N";
    private String paLoanClassificationClassificationType = "N";
    private String paLoanClassificationClassificationSection = "N";
    private String paLoanClassificationOrgNums = "N";
    private String otherM3FOrgNums = "0";
    private String otherM6FOrgNums = "0";
    private String otherM9FOrgNums = "0";
    private String otherM12FOrgNums = "0";
    private String otherM24FOrgNums = "0";
    private String bankM3FOrgNums = "0";
    private String bankM6FOrgNums = "0";
    private String bankM9FOrgNums = "0";
    private String bankM12FOrgNums = "0";
    private String bankM24FOrgNums = "0";


    //bqs
    private String bqsQueryDataQDId = "N";
    private String bqsQueryDataId = "N";
    private String bqsQueryDataFinalDecision = "N";
    private String bqsQueryDataBorrowerId = "N";

    private String bqsStrategyQDId = "N";
    private String bqsStrategyId = "N";

    private String bqsRuleStrategyId = "N";
    private String ruleID = "N";
    private String ruleID_378967 = "0";
    private String ruleID_378968 = "0";
    private String ruleID_378969 = "0";
    private String ruleID_378970 = "0";
    private String ruleID_378971 = "0";
    private String ruleID_378972 = "0";
    private String ruleID_378973 = "0";
    private String ruleID_378974 = "0";
    private String ruleID_378994 = "0";
    private String ruleID_378995 = "0";
    private String ruleID_379012 = "0";

    //zxt
    private String zxtHighestRiskQUId = "N";
    private String highestRiskLevelDescription = "N";
    private String highestRiskRevel = "N";
    private String zxtHighestRiskBorrowerId = "N";

    //sm
    private String smLoanQDId = "N";
    private String smLoanLoanAction = "N";
    private String smLoanPlatformType = "N";
    private String smLoanVariable = "N";
    private String d3 = "0";
    private String d7 = "0";
    private String d30 = "0";
    private String d60 = "0";
    private String d90 = "0";
    private String d180 = "0";
    private String total = "0";
    private String d3o = "0";
    private String d7o= "0";
    private String d30o = "0";
    private String d60o = "0";
    private String d90o = "0";
    private String d180o = "0";
    private String totalo = "0";
    private String registrationPlatformFD3 = "0";
    private String registrationPlatformFD7 = "0";
    private String registrationPlatformFD30 = "0";
    private String registrationPlatformFD60 = "0";
    private String registrationPlatformFD90 = "0";
    private String registrationPlatformFD180 = "0";
    private String registrationPlatformFTotal = "0";
    private String applyForALloanFD3 = "0";
    private String applyForALloanFD7 = "0";
    private String applyForALloanFD30 = "0";
    private String applyForALloanFD60 = "0";
    private String applyForALloanFD90 = "0";
    private String applyForALloanFD180 = "0";
    private String applyForALloanFTotal = "0";
    private String requestRejectionFD3 = "0";
    private String requestRejectionFD7 = "0";
    private String requestRejectionFD30 = "0";
    private String requestRejectionFD60 = "0";
    private String requestRejectionFD90 = "0";
    private String requestRejectionFD180 = "0";
    private String requestRejectionFTotal = "0";
    private String passFD3 = "0";
    private String passFD7 = "0";
    private String passFD30 = "0";
    private String passFD60 = "0";
    private String passFD90 = "0";
    private String passFD180 = "0";
    private String passFTotal = "0";

    private String smRelationId = "N";
    private String behaviorType = "N";

    private String smLendingQDId = "N";
    private String overallRiskLevel = "N";
    private String riskScore = "N";

    //other
    private String strategyName = "N";


    public WideTableWritable() {
    }

    public void textForWritable(String[] values) {
        this.tableName=values[0];
        this.machineSearchIdentify=values[1];
        this.machineSearchSN=values[2];
        this.queryDataFlowSn=values[3];
        this.tripartitePrimaryKey=values[4];
        this.borrowerBorrowerId=values[5];
        this.borrowerOrderSn=values[6];
        this.borrowerLoanType=values[7];
        this.borrowerBorrowerMoney=values[8];
        this.borrowerPayWay=values[9];
        this.borrowerBorrowerPeriod=values[10];
        this.borrowerName=values[11];
        this.borrowerIdCard=values[12];
        this.borrowerBankCard=values[13];
        this.borrowerMobile=values[14];
        this.infoOrderSn=values[15];
        this.borrowerInfoSex=values[16];
        this.borrowerInfoEducation=values[17];
        this.borrowerInfoMarriageStatus=values[18];
        this.borrowerInfoContactPhone=values[19];
        this.borrowerInfoLoanPurpose=values[20];
        this.borrowerInfoGuaranteeMeasure=values[21];
        this.borrowerInfoIncomeSource=values[22];
        this.borrowerInfoCompanyName=values[23];
        this.borrowerInfoCompanyAddress=values[24];
        this.borrowerInfoCompanyPhone=values[25];
        this.borrowerInfoProfession=values[26];
        this.contactOrderSn=values[27];
        this.borrowerContactEmergencyContactName=values[28];
        this.borrowerContactEmergencyContactRelation=values[29];
        this.borrowerContactEmergencyContactPhone=values[30];
        this.numberOfEmergencyContacts=values[31];
        this.extraOrderSn=values[32];
        this.additionalInformationOfPartner=values[33];
        this.loanApplicationTime=values[34];
        this.numberOfMobileLink=values[35];
        this.netTime=values[36];
        this.activeFrequency=values[37];
        this.aveCommunicationCost=values[38];
        this.hlslQueryDataId=values[39];
        this.hlslQueryDataQUId=values[40];
        this.hlslQueryDataBorrowerId=values[41];
        this.hlslQueryDataUserName=values[42];
        this.hlslQueryDataUserIdCard=values[43];
        this.hlslQueryDataUserPhone=values[44];
        this.hlslUserBasicQUID=values[45];
        this.hlslUserBasicAge=values[46];
        this.hlslUserBasicBirthday=values[47];
        this.hlslUserBasicGender=values[48];
        this.hlslUserBasicIdCardCity=values[49];
        this.hlslUserBasicIdCardProvince=values[50];
        this.hlslUserBasicIdCardRegion=values[51];
        this.hlslUserBasicIdCardValidate=values[52];
        this.hlslUserBasicLastAppearIdcard=values[53];
        this.hlslUserBasicLastAppearPhone=values[54];
        this.hlslUserBasicPhoneCity=values[55];
        this.hlslUserBasicPhoneOperator=values[56];
        this.hlslUserBasicPhoneProvince=values[57];
        this.hlslUserBasicRecordIdCardDays=values[58];
        this.hlslUserBasicRecordPhoneDays=values[59];
        this.hlslUserBasicUsedIdCardsCnt=values[60];
        this.hlslUserBasicUsedPhonesCnt=values[61];
        this.hlslHistoryOrgQUId=values[62];
        this.hlslHistoryOrgCreditCardRepaymentCnt=values[63];
        this.hlslHistoryOrgOfflineCashLoanCnt=values[64];
        this.hlslHistoryOrgOfflineInstallmentCnt=values[65];
        this.hlslHistoryOrgOnlineCashLoanCnt=values[66];
        this.hlslHistoryOrgOnlineInstallmentCnt=values[67];
        this.hlslHistoryOrgOthersCnt=values[68];
        this.hlslHistoryOrgPaydayLoanCnt=values[69];
        this.hlslHistorySearchQUId=values[70];
        this.hlsrHistorySearchOrgCnt=values[71];
        this.hlslHistorySearchSearchCntRecent14Days=values[72];
        this.hlslHistorySearchSearchCntRecent180Days=values[73];
        this.hlslHistorySearchSearchCntRecent30Days=values[74];
        this.hlslHistorySearchSearchCntRecent60Days=values[75];
        this.hlslHistorySearchSearchCntRecent7Days=values[76];
        this.hlslHistorySearchSearchCntRecent90Days=values[77];
        this.hlslHistorySearchSearchCnt=values[78];
        this.hlslHistorySearchOrgCntRecent14Days=values[79];
        this.hlslHistorySearchOrgCntRecent180Days=values[80];
        this.hlslHistorySearchOrgCntRecent30Days=values[81];
        this.hlslHistorySearchOrgCntRecent60Days=values[82];
        this.hlslHistorySearchOrgCntRecent7Days=values[83];
        this.hlslHistorySearchOrgCntRecent90Days=values[84];
        this.paLoanQueryDataQDId=values[85];
        this.paLoanQueryDataId=values[86];
        this.paLoanQueryDataBorrowerId=values[87];
        this.paLoanLoanRecordQDId=values[88];
        this.paLoanLoanRecordId=values[89];
        this.paLoanClassificationId=values[90];
        this.paLoanClassificationRId=values[91];
        this.paLoanClassificationClassificationType=values[92];
        this.paLoanClassificationClassificationSection=values[93];
        this.paLoanClassificationOrgNums=values[94];
        this.otherM3FOrgNums=values[95];
        this.otherM6FOrgNums=values[96];
        this.otherM9FOrgNums=values[97];
        this.otherM12FOrgNums=values[98];
        this.otherM24FOrgNums=values[99];
        this.bankM3FOrgNums=values[100];
        this.bankM6FOrgNums=values[101];
        this.bankM9FOrgNums=values[102];
        this.bankM12FOrgNums=values[103];
        this.bankM24FOrgNums=values[104];
        this.bqsQueryDataQDId=values[105];
        this.bqsQueryDataId=values[106];
        this.bqsQueryDataFinalDecision=values[107];
        this.bqsQueryDataBorrowerId=values[108];
        this.bqsStrategyQDId=values[109];
        this.bqsStrategyId=values[110];
        this.bqsRuleStrategyId=values[111];
        this.ruleID=values[112];
        this.ruleID_378967=values[113];
        this.ruleID_378968=values[114];
        this.ruleID_378969=values[115];
        this.ruleID_378970=values[116];
        this.ruleID_378971=values[117];
        this.ruleID_378972=values[118];
        this.ruleID_378973=values[119];
        this.ruleID_378974=values[120];
        this.ruleID_378994=values[121];
        this.ruleID_378995=values[122];
        this.ruleID_379012=values[123];
        this.zxtHighestRiskQUId=values[124];
        this.highestRiskLevelDescription=values[125];
        this.highestRiskRevel=values[126];
        this.zxtHighestRiskBorrowerId=values[127];
        this.smLoanQDId=values[128];
        this.smLoanLoanAction=values[129];
        this.smLoanPlatformType=values[130];
        this.smLoanVariable=values[131];
        this.d3=values[132];
        this.d7=values[133];
        this.d30=values[134];
        this.d60=values[135];
        this.d90=values[136];
        this.d180=values[137];
        this.total=values[138];
        this.d3o=values[139];
        this.d7o=values[140];
        this.d30o=values[141];
        this.d60o=values[142];
        this.d90o=values[143];
        this.d180o=values[144];
        this.totalo=values[145];
        this.registrationPlatformFD3=values[146];
        this.registrationPlatformFD7=values[147];
        this.registrationPlatformFD30=values[148];
        this.registrationPlatformFD60=values[149];
        this.registrationPlatformFD90=values[150];
        this.registrationPlatformFD180=values[151];
        this.registrationPlatformFTotal=values[152];
        this.applyForALloanFD3=values[153];
        this.applyForALloanFD7=values[154];
        this.applyForALloanFD30=values[155];
        this.applyForALloanFD60=values[156];
        this.applyForALloanFD90=values[157];
        this.applyForALloanFD180=values[158];
        this.applyForALloanFTotal=values[159];
        this.requestRejectionFD3=values[160];
        this.requestRejectionFD7=values[161];
        this.requestRejectionFD30=values[162];
        this.requestRejectionFD60=values[163];
        this.requestRejectionFD90=values[164];
        this.requestRejectionFD180=values[165];
        this.requestRejectionFTotal=values[166];
        this.passFD3=values[167];
        this.passFD7=values[168];
        this.passFD30=values[169];
        this.passFD60=values[170];
        this.passFD90=values[171];
        this.passFD180=values[172];
        this.passFTotal=values[173];
        this.smRelationId=values[174];
        this.behaviorType=values[175];
        this.smLendingQDId=values[176];
        this.overallRiskLevel=values[177];
        this.riskScore=values[178];
        this.strategyName=values[179];

    }

    public void textForWritable(String value) {
        String[] values = value.split(TableInfo.SPLITTER, -1);
        textForWritable(values);
    }


    public void setTRmeBorrower(String borrowerBorrowerId, String borrowerOrderSn, String borrowerLoanType, String borrowerBorrowerMoney, String borrowerPayWay, String borrowerBorrowerPeriod, String borrowerName, String borrowerIdCard, String borrowerBankCard, String borrowerMobile) {
        this.borrowerBorrowerId = borrowerBorrowerId;
        this.borrowerOrderSn = borrowerOrderSn;
        this.borrowerLoanType = borrowerLoanType;
        this.borrowerBorrowerMoney = borrowerBorrowerMoney;
        this.borrowerPayWay = borrowerPayWay;
        this.borrowerBorrowerPeriod = borrowerBorrowerPeriod;
        this.borrowerName = borrowerName;
        this.borrowerIdCard = borrowerIdCard;
        this.borrowerBankCard = borrowerBankCard;
        this.borrowerMobile = borrowerMobile;
    }

    public void setTBorrowerInfo(String infoOrderSn, String borrowerInfoSex, String borrowerInfoEducation, String borrowerInfoMarriageStatus, String borrowerInfoContactPhone, String borrowerInfoLoanPurpose, String borrowerInfoGuaranteeMeasure, String borrowerInfoIncomeSource, String borrowerInfoCompanyName, String borrowerInfoCompanyAddress, String borrowerInfoCompanyPhone, String borrowerInfoProfession) {
        this.infoOrderSn = infoOrderSn;
        this.borrowerInfoSex = borrowerInfoSex;
        this.borrowerInfoEducation = borrowerInfoEducation;
        this.borrowerInfoMarriageStatus = borrowerInfoMarriageStatus;
        this.borrowerInfoContactPhone = borrowerInfoContactPhone;
        this.borrowerInfoLoanPurpose = borrowerInfoLoanPurpose;
        this.borrowerInfoGuaranteeMeasure = borrowerInfoGuaranteeMeasure;
        this.borrowerInfoIncomeSource = borrowerInfoIncomeSource;
        this.borrowerInfoCompanyName = borrowerInfoCompanyName;
        this.borrowerInfoCompanyAddress = borrowerInfoCompanyAddress;
        this.borrowerInfoCompanyPhone = borrowerInfoCompanyPhone;
        this.borrowerInfoProfession = borrowerInfoProfession;
    }

    public void setTBorrowerContact(String contactOrderSn, String borrowerContactEmergencyContactName, String borrowerContactEmergencyContactRelation, String borrowerContactEmergencyContactPhone) {
        this.contactOrderSn = contactOrderSn;
        this.borrowerContactEmergencyContactName = borrowerContactEmergencyContactName;
        this.borrowerContactEmergencyContactRelation = borrowerContactEmergencyContactRelation;
        this.borrowerContactEmergencyContactPhone = borrowerContactEmergencyContactPhone;
        this.numberOfEmergencyContacts = numberOfEmergencyContacts;
    }

    public void setTBorrowerExtra(String extraOrderSn, String additionalInformationOfPartner) {
        this.extraOrderSn = extraOrderSn;
        //this.additionalInformationOfPartner = additionalInformationOfPartner;
        if (!"\\N".equals(additionalInformationOfPartner) && !"N".equals(additionalInformationOfPartner) && !"[]".equals(additionalInformationOfPartner) && !"{}".equals(additionalInformationOfPartner) && !additionalInformationOfPartner.isEmpty() && isJson(additionalInformationOfPartner)) {
            JSONObject jsonDataMap = JSON.parseObject(additionalInformationOfPartner);
            if (jsonDataMap == null || jsonDataMap.isEmpty()) {
                this.additionalInformationOfPartner = additionalInformationOfPartner;
                return;
            }
            if (jsonDataMap.get("loan_application_time") != null) {
                loanApplicationTime = jsonDataMap.get("loan_application_time").toString();
            }
            if (jsonDataMap.get("number_of_mobile_link") != null) {
                numberOfMobileLink = jsonDataMap.get("number_of_mobile_link").toString();
            }
            if (jsonDataMap.get("net_time") != null) {
                netTime = jsonDataMap.get("net_time").toString();
            }
            if (jsonDataMap.get("active_frequency") != null) {
                activeFrequency = jsonDataMap.get("active_frequency").toString();
            }
            if (jsonDataMap.get("ave_communication_cost") != null) {
                aveCommunicationCost = (String) jsonDataMap.get("ave_communication_cost");
            }
        }
    }

    public void setTBorrowerExtraNoInfo(String extraOrderSn, String loanApplicationTime, String numberOfMobileLink, String netTime, String activeFrequency, String aveCommunicationCost) {
        this.extraOrderSn = extraOrderSn;
        this.loanApplicationTime = loanApplicationTime;
        this.numberOfMobileLink = numberOfMobileLink;
        this.netTime = netTime;
        this.activeFrequency = activeFrequency;
        this.aveCommunicationCost = aveCommunicationCost;
    }


    public void setTMachineSearchFlow(String machineSearchIdentify, String machineSearchSN) {
        this.machineSearchIdentify = machineSearchIdentify;
        this.machineSearchSN = machineSearchSN;
    }

    public void setTQueryData(String tripartitePrimaryKey, String queryDataFlowSn) {
        this.queryDataFlowSn = queryDataFlowSn;
        this.tripartitePrimaryKey = tripartitePrimaryKey;
    }


    public void setTHlslQueryData(String hlslQueryDataId, String hlslQueryDataQUId, String hlslQueryDataBorrowerId, String hlslQueryDataUserName, String hlslQueryDataUserIdCard, String hlslQueryDataUserPhone) {
        this.hlslQueryDataId = hlslQueryDataId;
        this.hlslQueryDataQUId = hlslQueryDataQUId;
        this.hlslQueryDataBorrowerId = hlslQueryDataBorrowerId;
        this.hlslQueryDataUserName = hlslQueryDataUserName;
        this.hlslQueryDataUserIdCard = hlslQueryDataUserIdCard;
        this.hlslQueryDataUserPhone = hlslQueryDataUserPhone;
    }

    public void setTHlslUserBasic(String hlslUserBasicQUID, String hlslUserBasicAge, String hlslUserBasicBirthday, String hlslUserBasicGender, String hlslUserBasicIdCardCity, String hlslUserBasicIdCardProvince, String hlslUserBasicIdCardRegion, String hlslUserBasicIdCardValidate, String hlslUserBasicLastAppearIdcard, String hlslUserBasicLastAppearPhone, String hlslUserBasicPhoneCity, String hlslUserBasicPhoneOperator, String hlslUserBasicPhoneProvince, String hlslUserBasicRecordIdCardDays, String hlslUserBasicRecordPhoneDays, String hlslUserBasicUsedIdCardsCnt, String hlslUserBasicUsedPhonesCnt) {
        this.hlslUserBasicQUID = hlslUserBasicQUID;
        this.hlslUserBasicAge = hlslUserBasicAge;
        this.hlslUserBasicBirthday = hlslUserBasicBirthday;
        this.hlslUserBasicGender = hlslUserBasicGender;
        this.hlslUserBasicIdCardCity = hlslUserBasicIdCardCity;
        this.hlslUserBasicIdCardProvince = hlslUserBasicIdCardProvince;
        this.hlslUserBasicIdCardRegion = hlslUserBasicIdCardRegion;
        this.hlslUserBasicIdCardValidate = hlslUserBasicIdCardValidate;
        this.hlslUserBasicLastAppearIdcard = hlslUserBasicLastAppearIdcard;
        this.hlslUserBasicLastAppearPhone = hlslUserBasicLastAppearPhone;
        this.hlslUserBasicPhoneCity = hlslUserBasicPhoneCity;
        this.hlslUserBasicPhoneOperator = hlslUserBasicPhoneOperator;
        this.hlslUserBasicPhoneProvince = hlslUserBasicPhoneProvince;
        this.hlslUserBasicRecordIdCardDays = hlslUserBasicRecordIdCardDays;
        this.hlslUserBasicRecordPhoneDays = hlslUserBasicRecordPhoneDays;
        this.hlslUserBasicUsedIdCardsCnt = hlslUserBasicUsedIdCardsCnt;
        this.hlslUserBasicUsedPhonesCnt = hlslUserBasicUsedPhonesCnt;
    }

    public void setTHlslHistoryOrg(String hlslHistoryOrgQUId, String hlslHistoryOrgCreditCardRepaymentCnt, String hlslHistoryOrgOfflineCashLoanCnt, String hlslHistoryOrgOfflineInstallmentCnt, String hlslHistoryOrgOnlineCashLoanCnt, String hlslHistoryOrgOnlineInstallmentCnt, String hlslHistoryOrgOthersCnt, String hlslHistoryOrgPaydayLoanCnt) {
        this.hlslHistoryOrgQUId = hlslHistoryOrgQUId;
        this.hlslHistoryOrgCreditCardRepaymentCnt = hlslHistoryOrgCreditCardRepaymentCnt;
        this.hlslHistoryOrgOfflineCashLoanCnt = hlslHistoryOrgOfflineCashLoanCnt;
        this.hlslHistoryOrgOfflineInstallmentCnt = hlslHistoryOrgOfflineInstallmentCnt;
        this.hlslHistoryOrgOnlineCashLoanCnt = hlslHistoryOrgOnlineCashLoanCnt;
        this.hlslHistoryOrgOnlineInstallmentCnt = hlslHistoryOrgOnlineInstallmentCnt;
        this.hlslHistoryOrgOthersCnt = hlslHistoryOrgOthersCnt;
        this.hlslHistoryOrgPaydayLoanCnt = hlslHistoryOrgPaydayLoanCnt;
    }

    public void setTHlslHistorySearch(String hlslHistorySearchQUId, String hlsrHistorySearchOrgCnt, String hlslHistorySearchSearchCntRecent14Days, String hlslHistorySearchSearchCntRecent180Days, String hlslHistorySearchSearchCntRecent30Days, String hlslHistorySearchSearchCntRecent60Days, String hlslHistorySearchSearchCntRecent7Days, String hlslHistorySearchSearchCntRecent90Days, String hlslHistorySearchSearchCnt, String hlslHistorySearchOrgCntRecent14Days, String hlslHistorySearchOrgCntRecent180Days, String hlslHistorySearchOrgCntRecent30Days, String hlslHistorySearchOrgCntRecent60Days, String hlslHistorySearchOrgCntRecent7Days, String hlslHistorySearchOrgCntRecent90Days) {
        this.hlslHistorySearchQUId = hlslHistorySearchQUId;
        this.hlsrHistorySearchOrgCnt = hlsrHistorySearchOrgCnt;
        this.hlslHistorySearchSearchCntRecent14Days = hlslHistorySearchSearchCntRecent14Days;
        this.hlslHistorySearchSearchCntRecent180Days = hlslHistorySearchSearchCntRecent180Days;
        this.hlslHistorySearchSearchCntRecent30Days = hlslHistorySearchSearchCntRecent30Days;
        this.hlslHistorySearchSearchCntRecent60Days = hlslHistorySearchSearchCntRecent60Days;
        this.hlslHistorySearchSearchCntRecent7Days = hlslHistorySearchSearchCntRecent7Days;
        this.hlslHistorySearchSearchCntRecent90Days = hlslHistorySearchSearchCntRecent90Days;
        this.hlslHistorySearchSearchCnt = hlslHistorySearchSearchCnt;
        this.hlslHistorySearchOrgCntRecent14Days = hlslHistorySearchOrgCntRecent14Days;
        this.hlslHistorySearchOrgCntRecent180Days = hlslHistorySearchOrgCntRecent180Days;
        this.hlslHistorySearchOrgCntRecent30Days = hlslHistorySearchOrgCntRecent30Days;
        this.hlslHistorySearchOrgCntRecent60Days = hlslHistorySearchOrgCntRecent60Days;
        this.hlslHistorySearchOrgCntRecent7Days = hlslHistorySearchOrgCntRecent7Days;
        this.hlslHistorySearchOrgCntRecent90Days = hlslHistorySearchOrgCntRecent90Days;
    }


    public void setTZxtHighestRisk(String zxtHighestRiskQUId, String highestRiskLevelDescription, String highestRiskRevel, String zxtHighestRiskBorrowerId) {
        this.zxtHighestRiskQUId = zxtHighestRiskQUId;
        this.highestRiskLevelDescription = highestRiskLevelDescription;
        this.highestRiskRevel = highestRiskRevel;
        this.zxtHighestRiskBorrowerId = zxtHighestRiskBorrowerId;
    }


    public void setTPaLoanQueryData(String paLoanQueryDataQDId, String paLoanQueryDataId, String paLoanQueryDataBorrowerId) {
        this.paLoanQueryDataQDId = paLoanQueryDataQDId;
        this.paLoanQueryDataId = paLoanQueryDataId;
        this.paLoanQueryDataBorrowerId = paLoanQueryDataBorrowerId;
    }

    public void setTPaLoanRecord(String paLoanLoanRecordQDId, String paLoanLoanRecordId) {
        this.paLoanLoanRecordQDId = paLoanLoanRecordQDId;
        this.paLoanLoanRecordId = paLoanLoanRecordId;
    }

    public void setTPaLoanClassification(String paLoanLoanClassificationId, String paLoanLoanClassificationRId, String paLoanClassificationClassificationType, String paLoanClassificationClassificationSection, String paLoanClassificationOrgNums) {
        this.paLoanClassificationId = paLoanLoanClassificationId;
        this.paLoanClassificationRId = paLoanLoanClassificationRId;
        this.paLoanClassificationClassificationType = paLoanClassificationClassificationType;
        this.paLoanClassificationClassificationSection = paLoanClassificationClassificationSection;
        this.paLoanClassificationOrgNums = paLoanClassificationOrgNums;
        if (StringUtils.equalsIgnoreCase(paLoanClassificationClassificationType, "other")) {
            switch (paLoanClassificationClassificationSection) {
                case "M3":
                    this.otherM3FOrgNums = "" + NumberUtils.toInt(this.otherM3FOrgNums) + paLoanClassificationOrgNums;
                    break;
                case "M6":
                    this.otherM6FOrgNums = "" + NumberUtils.toInt(this.otherM6FOrgNums) + paLoanClassificationOrgNums;
                    break;
                case "M9":
                    this.otherM9FOrgNums = "" + NumberUtils.toInt(this.otherM9FOrgNums) + paLoanClassificationOrgNums;
                    break;
                case "M12":
                    this.otherM12FOrgNums = "" + NumberUtils.toInt(this.otherM12FOrgNums) + paLoanClassificationOrgNums;
                    break;
                case "M24":
                    this.otherM24FOrgNums = "" + NumberUtils.toInt(this.otherM24FOrgNums) + paLoanClassificationOrgNums;
                    break;
                default:
                    break;
            }
        } else if (StringUtils.equalsIgnoreCase(paLoanClassificationClassificationType, "bank")) {
            switch (paLoanClassificationClassificationSection) {
                case "M3":
                    this.bankM3FOrgNums = "" + NumberUtils.toInt(this.bankM3FOrgNums) + paLoanClassificationOrgNums;
                    break;
                case "M6":
                    this.bankM6FOrgNums = "" + NumberUtils.toInt(this.bankM6FOrgNums) + paLoanClassificationOrgNums;
                    break;
                case "M9":
                    this.bankM9FOrgNums = "" + NumberUtils.toInt(this.bankM9FOrgNums) + paLoanClassificationOrgNums;
                    break;
                case "M12":
                    this.bankM12FOrgNums = "" + NumberUtils.toInt(this.bankM12FOrgNums) + paLoanClassificationOrgNums;
                    break;
                case "M24":
                    this.bankM24FOrgNums = "" + NumberUtils.toInt(this.bankM24FOrgNums) + paLoanClassificationOrgNums;
                    break;
                default:
                    break;
            }
        }
    }

    public void setPaClassInfo(WideTableWritable wideTableWritable) {
        this.bankM3FOrgNums = wideTableWritable.getBankM3FOrgNums();
        this.bankM6FOrgNums = wideTableWritable.getBankM6FOrgNums();
        this.bankM9FOrgNums = wideTableWritable.getBankM9FOrgNums();
        this.bankM12FOrgNums = wideTableWritable.getBankM12FOrgNums();
        this.bankM24FOrgNums = wideTableWritable.getBankM24FOrgNums();
        this.otherM3FOrgNums = wideTableWritable.getOtherM3FOrgNums();
        this.otherM6FOrgNums = wideTableWritable.getOtherM6FOrgNums();
        this.otherM9FOrgNums = wideTableWritable.getOtherM9FOrgNums();
        this.otherM12FOrgNums = wideTableWritable.getOtherM12FOrgNums();
        this.otherM24FOrgNums = wideTableWritable.getOtherM24FOrgNums();
    }


    public void setTBqsQueryData(String bqsQueryDataQDId, String bqsQueryDataId, String bqsQueryDataFinalDecision, String bqsQueryDataBorrowerId) {
        this.bqsQueryDataQDId = bqsQueryDataQDId;
        this.bqsQueryDataId = bqsQueryDataId;
        this.bqsQueryDataFinalDecision = bqsQueryDataFinalDecision;
        this.bqsQueryDataBorrowerId = bqsQueryDataBorrowerId;
    }

    public void setTBqsStrategy(String bqsStrategyQDId, String bqsStrategyId, String strategyName) {
        this.bqsStrategyQDId = bqsStrategyQDId;
        this.bqsStrategyId = bqsStrategyId;
        this.strategyName = strategyName;
    }

    public void setTBqsRule(String bqsRuleStrategyId, String ruleID) {
        this.bqsRuleStrategyId = bqsRuleStrategyId;
        this.ruleID = ruleID;
        switch (ruleID) {
            case "378967":
                this.ruleID_378967 = "1";
                break;
            case "378968":
                this.ruleID_378968 = "1" ;
                break;
            case "378969":
                this.ruleID_378969 = "1" ;
                break;
            case "378970":
                this.ruleID_378970 = "1" ;
                break;
            case "378971":
                this.ruleID_378971 = "1";
                break;
            case "378972":
                this.ruleID_378972 = "1";
                break;
            case "378973":
                this.ruleID_378973 = "1";
                break;
            case "378974":
                this.ruleID_378974 = "1";
                break;
            case "378994":
                this.ruleID_378994 = "1";
                break;
            case "378995":
                this.ruleID_378995 = "1";
                break;
            case "379012":
                this.ruleID_379012 = "1";
                break;
            default:
                break;
        }
    }

    public void setBqsRuleInfo(WideTableWritable wideTableWritable) {
        if (!wideTableWritable.getRuleID_378968().equals("0")) {
            this.ruleID_378968 = (wideTableWritable.getRuleID_378968());
        }
        if (!wideTableWritable.getRuleID_378969().equals("0")) {
            this.ruleID_378969 = (wideTableWritable.getRuleID_378969());
        }
        if (!wideTableWritable.getRuleID_378970().equals("0")) {
            this.ruleID_378970=(wideTableWritable.getRuleID_378970());
        }
        if (!wideTableWritable.getRuleID_378971().equals("0")) {
            this.ruleID_378971=(wideTableWritable.getRuleID_378971());
        }
        if (!wideTableWritable.getRuleID_378972().equals("0")) {
            this.ruleID_378972=(wideTableWritable.getRuleID_378972());
        }
        if (!wideTableWritable.getRuleID_378973().equals("0")) {
            this.ruleID_378973=(wideTableWritable.getRuleID_378973());
        }
        if (!wideTableWritable.getRuleID_378974().equals("0")) {
            this.ruleID_378974=(wideTableWritable.getRuleID_378974());
        }
        if (!wideTableWritable.getRuleID_378994().equals("0")) {
            this.ruleID_378994=(wideTableWritable.getRuleID_378994());
        }
        if (!wideTableWritable.getRuleID_378995().equals("0")) {
            this.ruleID_378995=(wideTableWritable.getRuleID_378995());
        }
        if (!wideTableWritable.getRuleID_379012().equals("0")) {
            this.ruleID_379012=(wideTableWritable.getRuleID_379012());
        }
    }


    public void setTSmLoan(String smLoanQDId, String smLoanLoanAction, String smLoanPlatformType, String smLoanVariable, String d3, String d7, String d30, String d60, String d90, String d180, String total) {
        this.smLoanQDId = smLoanQDId;
        this.smLoanLoanAction = smLoanLoanAction;
        this.smLoanPlatformType = smLoanPlatformType;
        this.smLoanVariable = smLoanVariable;
        this.d3 = d3;
        this.d7 = d7;
        this.d30 = d30;
        this.d60 = d60;
        this.d90 = d90;
        this.d180 = d180;
        this.total = total;
        if (StringUtils.equals(smLoanLoanAction, "42") && StringUtils.equals(smLoanPlatformType, "41") && StringUtils.equals(smLoanVariable, "43")) {
            this.d3o = ""+(NumberUtils.toInt(this.d3o)+NumberUtils.toInt(d3));
            this.d7o = ""+(NumberUtils.toInt(this.d7o)+NumberUtils.toInt(d7));
            this.d30o = ""+(NumberUtils.toInt(this.d30o)+NumberUtils.toInt(d30));
            this.d60o = ""+(NumberUtils.toInt(this.d60o)+NumberUtils.toInt(d60));
            this.d90o = ""+(NumberUtils.toInt(this.d90o)+NumberUtils.toInt(d90));
            this.d180o = ""+(NumberUtils.toInt(this.d180o)+NumberUtils.toInt(d180));
            this.totalo = ""+(NumberUtils.toInt(this.totalo)+NumberUtils.toInt(total));
        }
        switch (smLoanLoanAction) {
            case "30":
                this.registrationPlatformFD3 = "" + (NumberUtils.toInt(this.registrationPlatformFD3) + NumberUtils.toInt(d3));
                this.registrationPlatformFD7 = "" + (NumberUtils.toInt(this.registrationPlatformFD7) + NumberUtils.toInt(d7));
                this.registrationPlatformFD30 = "" + (NumberUtils.toInt(this.registrationPlatformFD30) + NumberUtils.toInt(d30));
                this.registrationPlatformFD60 = "" + (NumberUtils.toInt(this.registrationPlatformFD60) + NumberUtils.toInt(d60));
                this.registrationPlatformFD90 = "" + (NumberUtils.toInt(this.registrationPlatformFD90) + NumberUtils.toInt(d90));
                this.registrationPlatformFD180 = "" + (NumberUtils.toInt(this.registrationPlatformFD180) + NumberUtils.toInt(d180));
                this.registrationPlatformFTotal = "" + (NumberUtils.toInt(this.registrationPlatformFTotal) + NumberUtils.toInt(total));
                break;
            case "31":
                this.applyForALloanFD3 = "" + (NumberUtils.toInt(this.applyForALloanFD3) + NumberUtils.toInt(d3));
                this.applyForALloanFD7 = "" + (NumberUtils.toInt(this.applyForALloanFD7) + NumberUtils.toInt(d7));
                this.applyForALloanFD30 = "" + (NumberUtils.toInt(this.applyForALloanFD30) + NumberUtils.toInt(d30));
                this.applyForALloanFD60 = "" + (NumberUtils.toInt(this.applyForALloanFD60) + NumberUtils.toInt(d60));
                this.applyForALloanFD90 = "" + (NumberUtils.toInt(this.applyForALloanFD90) + NumberUtils.toInt(d90));
                this.applyForALloanFD180 = "" + (NumberUtils.toInt(this.applyForALloanFD180) + NumberUtils.toInt(d180));
                this.applyForALloanFTotal = "" + (NumberUtils.toInt(this.applyForALloanFTotal) + NumberUtils.toInt(total));
                break;
            case "33":
                this.requestRejectionFD3 = "" + (NumberUtils.toInt(this.requestRejectionFD3) + NumberUtils.toInt(d3));
                this.requestRejectionFD7 = "" + (NumberUtils.toInt(this.requestRejectionFD7) + NumberUtils.toInt(d7));
                this.requestRejectionFD30 = "" + (NumberUtils.toInt(this.requestRejectionFD30) + NumberUtils.toInt(d30));
                this.requestRejectionFD60 = "" + (NumberUtils.toInt(this.requestRejectionFD60) + NumberUtils.toInt(d60));
                this.requestRejectionFD90 = "" + (NumberUtils.toInt(this.requestRejectionFD90) + NumberUtils.toInt(d90));
                this.requestRejectionFD180 = "" + (NumberUtils.toInt(this.requestRejectionFD180) + NumberUtils.toInt(d180));
                this.requestRejectionFTotal = "" + (NumberUtils.toInt(this.requestRejectionFTotal) + NumberUtils.toInt(total));
                break;
            case "32":
                this.passFD3 = "" + (NumberUtils.toInt(this.passFD3) + NumberUtils.toInt(d3));
                this.passFD7 = "" + (NumberUtils.toInt(this.passFD7) + NumberUtils.toInt(d7));
                this.passFD30 = "" + (NumberUtils.toInt(this.passFD30) + NumberUtils.toInt(d30));
                this.passFD60 = "" + (NumberUtils.toInt(this.passFD60) + NumberUtils.toInt(d60));
                this.passFD90 = "" + (NumberUtils.toInt(this.passFD90) + NumberUtils.toInt(d90));
                this.passFD180 = "" + (NumberUtils.toInt(this.passFD180) + NumberUtils.toInt(d180));
                this.passFTotal = "" + (NumberUtils.toInt(this.passFTotal) + NumberUtils.toInt(total));
                break;
            default:
                break;
        }
    }

    public void setTSmRelation(String smRelationId, String behaviorType) {
        this.smRelationId = smRelationId;
        this.behaviorType = behaviorType;
    }

    public void setTSmLending(String smLendingQDId, String overallRiskLevel, String riskScore) {
        this.smLendingQDId = smLendingQDId;
        this.overallRiskLevel = overallRiskLevel;
        this.riskScore = riskScore;
    }

    public void setSmInfo(WideTableWritable wideTableWritable) {
        this.smRelationId = wideTableWritable.getSmRelationId();
        this.behaviorType = wideTableWritable.getBehaviorType();
        this.smLendingQDId = wideTableWritable.getSmLendingQDId();
        this.overallRiskLevel = wideTableWritable.getOverallRiskLevel();
        this.riskScore = wideTableWritable.getRiskScore();
        this.smLoanQDId = wideTableWritable.getSmLoanQDId();
        this.smLoanLoanAction = wideTableWritable.getSmLoanLoanAction();
        this.smLoanPlatformType = wideTableWritable.getSmLoanPlatformType();
        this.smLoanVariable = wideTableWritable.getSmLoanVariable();
        this.d3 = wideTableWritable.getD3();
        this.d7 = wideTableWritable.getD7();
        this.d30 = wideTableWritable.getD30();
        this.d60 = wideTableWritable.getD60();
        this.d90 = wideTableWritable.getD90();
        this.d180 = wideTableWritable.getD180();
        this.total = wideTableWritable.getTotal();
        this.d3o = wideTableWritable.getD3o();
        this.d7o = wideTableWritable.getD7o();
        this.d30o = wideTableWritable.getD30o();
        this.d60o = wideTableWritable.getD60o();
        this.d90o = wideTableWritable.getD90o();
        this.d180o = wideTableWritable.getD180o();
        this.totalo = wideTableWritable.getTotalo();
        this.registrationPlatformFD3 = wideTableWritable.getRegistrationPlatformFD3();
        this.registrationPlatformFD7 = wideTableWritable.getRegistrationPlatformFD7();
        this.registrationPlatformFD30 = wideTableWritable.getRegistrationPlatformFD30();
        this.registrationPlatformFD60 = wideTableWritable.getRegistrationPlatformFD60();
        this.registrationPlatformFD90 = wideTableWritable.getRegistrationPlatformFD90();
        this.registrationPlatformFD180 = wideTableWritable.getRegistrationPlatformFD180();
        this.registrationPlatformFTotal = wideTableWritable.getRegistrationPlatformFTotal();
        this.applyForALloanFD3 = wideTableWritable.getApplyForALloanFD3();
        this.applyForALloanFD7 = wideTableWritable.getApplyForALloanFD7();
        this.applyForALloanFD30 = wideTableWritable.getApplyForALloanFD30();
        this.applyForALloanFD60 = wideTableWritable.getApplyForALloanFD60();
        this.applyForALloanFD90 = wideTableWritable.getApplyForALloanFD90();
        this.applyForALloanFD180 = wideTableWritable.getApplyForALloanFD180();
        this.applyForALloanFTotal = wideTableWritable.getApplyForALloanFTotal();
        this.requestRejectionFD3 = wideTableWritable.getRequestRejectionFD3();
        this.requestRejectionFD7 = wideTableWritable.getRequestRejectionFD7();
        this.requestRejectionFD30 = wideTableWritable.getRequestRejectionFD30();
        this.requestRejectionFD60 = wideTableWritable.getRequestRejectionFD60();
        this.requestRejectionFD90 = wideTableWritable.getRequestRejectionFD90();
        this.requestRejectionFD180 = wideTableWritable.getRequestRejectionFD180();
        this.requestRejectionFTotal = wideTableWritable.getRequestRejectionFTotal();
        this.passFD3 = wideTableWritable.getPassFD3();
        this.passFD7 = wideTableWritable.getPassFD7();
        this.passFD30 = wideTableWritable.getPassFD30();
        this.passFD60 = wideTableWritable.getPassFD60();
        this.passFD90 = wideTableWritable.getPassFD90();
        this.passFD180 = wideTableWritable.getPassFD180();
        this.passFTotal = wideTableWritable.getPassFTotal();

    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.tableName);
        dataOutput.writeUTF(this.machineSearchIdentify);
        dataOutput.writeUTF(this.machineSearchSN);
        dataOutput.writeUTF(this.queryDataFlowSn);
        dataOutput.writeUTF(this.tripartitePrimaryKey);
        dataOutput.writeUTF(this.borrowerBorrowerId);
        dataOutput.writeUTF(this.borrowerOrderSn);
        dataOutput.writeUTF(this.borrowerLoanType);
        dataOutput.writeUTF(this.borrowerBorrowerMoney);
        dataOutput.writeUTF(this.borrowerPayWay);
        dataOutput.writeUTF(this.borrowerBorrowerPeriod);
        dataOutput.writeUTF(this.borrowerName);
        dataOutput.writeUTF(this.borrowerIdCard);
        dataOutput.writeUTF(this.borrowerBankCard);
        dataOutput.writeUTF(this.borrowerMobile);
        dataOutput.writeUTF(this.infoOrderSn);
        dataOutput.writeUTF(this.borrowerInfoSex);
        dataOutput.writeUTF(this.borrowerInfoEducation);
        dataOutput.writeUTF(this.borrowerInfoMarriageStatus);
        dataOutput.writeUTF(this.borrowerInfoContactPhone);
        dataOutput.writeUTF(this.borrowerInfoLoanPurpose);
        dataOutput.writeUTF(this.borrowerInfoGuaranteeMeasure);
        dataOutput.writeUTF(this.borrowerInfoIncomeSource);
        dataOutput.writeUTF(this.borrowerInfoCompanyName);
        dataOutput.writeUTF(this.borrowerInfoCompanyAddress);
        dataOutput.writeUTF(this.borrowerInfoCompanyPhone);
        dataOutput.writeUTF(this.borrowerInfoProfession);
        dataOutput.writeUTF(this.contactOrderSn);
        dataOutput.writeUTF(this.borrowerContactEmergencyContactName);
        dataOutput.writeUTF(this.borrowerContactEmergencyContactRelation);
        dataOutput.writeUTF(this.borrowerContactEmergencyContactPhone);
        dataOutput.writeUTF(this.numberOfEmergencyContacts);
        dataOutput.writeUTF(this.extraOrderSn);
        dataOutput.writeUTF(this.additionalInformationOfPartner);
        dataOutput.writeUTF(this.loanApplicationTime);
        dataOutput.writeUTF(this.numberOfMobileLink);
        dataOutput.writeUTF(this.netTime);
        dataOutput.writeUTF(this.activeFrequency);
        dataOutput.writeUTF(this.aveCommunicationCost);
        dataOutput.writeUTF(this.hlslQueryDataId);
        dataOutput.writeUTF(this.hlslQueryDataQUId);
        dataOutput.writeUTF(this.hlslQueryDataBorrowerId);
        dataOutput.writeUTF(this.hlslQueryDataUserName);
        dataOutput.writeUTF(this.hlslQueryDataUserIdCard);
        dataOutput.writeUTF(this.hlslQueryDataUserPhone);
        dataOutput.writeUTF(this.hlslUserBasicQUID);
        dataOutput.writeUTF(this.hlslUserBasicAge);
        dataOutput.writeUTF(this.hlslUserBasicBirthday);
        dataOutput.writeUTF(this.hlslUserBasicGender);
        dataOutput.writeUTF(this.hlslUserBasicIdCardCity);
        dataOutput.writeUTF(this.hlslUserBasicIdCardProvince);
        dataOutput.writeUTF(this.hlslUserBasicIdCardRegion);
        dataOutput.writeUTF(this.hlslUserBasicIdCardValidate);
        dataOutput.writeUTF(this.hlslUserBasicLastAppearIdcard);
        dataOutput.writeUTF(this.hlslUserBasicLastAppearPhone);
        dataOutput.writeUTF(this.hlslUserBasicPhoneCity);
        dataOutput.writeUTF(this.hlslUserBasicPhoneOperator);
        dataOutput.writeUTF(this.hlslUserBasicPhoneProvince);
        dataOutput.writeUTF(this.hlslUserBasicRecordIdCardDays);
        dataOutput.writeUTF(this.hlslUserBasicRecordPhoneDays);
        dataOutput.writeUTF(this.hlslUserBasicUsedIdCardsCnt);
        dataOutput.writeUTF(this.hlslUserBasicUsedPhonesCnt);
        dataOutput.writeUTF(this.hlslHistoryOrgQUId);
        dataOutput.writeUTF(this.hlslHistoryOrgCreditCardRepaymentCnt);
        dataOutput.writeUTF(this.hlslHistoryOrgOfflineCashLoanCnt);
        dataOutput.writeUTF(this.hlslHistoryOrgOfflineInstallmentCnt);
        dataOutput.writeUTF(this.hlslHistoryOrgOnlineCashLoanCnt);
        dataOutput.writeUTF(this.hlslHistoryOrgOnlineInstallmentCnt);
        dataOutput.writeUTF(this.hlslHistoryOrgOthersCnt);
        dataOutput.writeUTF(this.hlslHistoryOrgPaydayLoanCnt);
        dataOutput.writeUTF(this.hlslHistorySearchQUId);
        dataOutput.writeUTF(this.hlsrHistorySearchOrgCnt);
        dataOutput.writeUTF(this.hlslHistorySearchSearchCntRecent14Days);
        dataOutput.writeUTF(this.hlslHistorySearchSearchCntRecent180Days);
        dataOutput.writeUTF(this.hlslHistorySearchSearchCntRecent30Days);
        dataOutput.writeUTF(this.hlslHistorySearchSearchCntRecent60Days);
        dataOutput.writeUTF(this.hlslHistorySearchSearchCntRecent7Days);
        dataOutput.writeUTF(this.hlslHistorySearchSearchCntRecent90Days);
        dataOutput.writeUTF(this.hlslHistorySearchSearchCnt);
        dataOutput.writeUTF(this.hlslHistorySearchOrgCntRecent14Days);
        dataOutput.writeUTF(this.hlslHistorySearchOrgCntRecent180Days);
        dataOutput.writeUTF(this.hlslHistorySearchOrgCntRecent30Days);
        dataOutput.writeUTF(this.hlslHistorySearchOrgCntRecent60Days);
        dataOutput.writeUTF(this.hlslHistorySearchOrgCntRecent7Days);
        dataOutput.writeUTF(this.hlslHistorySearchOrgCntRecent90Days);
        dataOutput.writeUTF(this.paLoanQueryDataQDId);
        dataOutput.writeUTF(this.paLoanQueryDataId);
        dataOutput.writeUTF(this.paLoanQueryDataBorrowerId);
        dataOutput.writeUTF(this.paLoanLoanRecordQDId);
        dataOutput.writeUTF(this.paLoanLoanRecordId);
        dataOutput.writeUTF(this.paLoanClassificationId);
        dataOutput.writeUTF(this.paLoanClassificationRId);
        dataOutput.writeUTF(this.paLoanClassificationClassificationType);
        dataOutput.writeUTF(this.paLoanClassificationClassificationSection);
        dataOutput.writeUTF(this.paLoanClassificationOrgNums);
        dataOutput.writeUTF(this.otherM3FOrgNums);
        dataOutput.writeUTF(this.otherM6FOrgNums);
        dataOutput.writeUTF(this.otherM9FOrgNums);
        dataOutput.writeUTF(this.otherM12FOrgNums);
        dataOutput.writeUTF(this.otherM24FOrgNums);
        dataOutput.writeUTF(this.bankM3FOrgNums);
        dataOutput.writeUTF(this.bankM6FOrgNums);
        dataOutput.writeUTF(this.bankM9FOrgNums);
        dataOutput.writeUTF(this.bankM12FOrgNums);
        dataOutput.writeUTF(this.bankM24FOrgNums);
        dataOutput.writeUTF(this.bqsQueryDataQDId);
        dataOutput.writeUTF(this.bqsQueryDataId);
        dataOutput.writeUTF(this.bqsQueryDataFinalDecision);
        dataOutput.writeUTF(this.bqsQueryDataBorrowerId);
        dataOutput.writeUTF(this.bqsStrategyQDId);
        dataOutput.writeUTF(this.bqsStrategyId);
        dataOutput.writeUTF(this.bqsRuleStrategyId);
        dataOutput.writeUTF(this.ruleID);
        dataOutput.writeUTF(this.ruleID_378967);
        dataOutput.writeUTF(this.ruleID_378968);
        dataOutput.writeUTF(this.ruleID_378969);
        dataOutput.writeUTF(this.ruleID_378970);
        dataOutput.writeUTF(this.ruleID_378971);
        dataOutput.writeUTF(this.ruleID_378972);
        dataOutput.writeUTF(this.ruleID_378973);
        dataOutput.writeUTF(this.ruleID_378974);
        dataOutput.writeUTF(this.ruleID_378994);
        dataOutput.writeUTF(this.ruleID_378995);
        dataOutput.writeUTF(this.ruleID_379012);
        dataOutput.writeUTF(this.zxtHighestRiskQUId);
        dataOutput.writeUTF(this.highestRiskLevelDescription);
        dataOutput.writeUTF(this.highestRiskRevel);
        dataOutput.writeUTF(this.zxtHighestRiskBorrowerId);
        dataOutput.writeUTF(this.smLoanQDId);
        dataOutput.writeUTF(this.smLoanLoanAction);
        dataOutput.writeUTF(this.smLoanPlatformType);
        dataOutput.writeUTF(this.smLoanVariable);
        dataOutput.writeUTF(this.d3);
        dataOutput.writeUTF(this.d7);
        dataOutput.writeUTF(this.d30);
        dataOutput.writeUTF(this.d60);
        dataOutput.writeUTF(this.d90);
        dataOutput.writeUTF(this.d180);
        dataOutput.writeUTF(this.total);
        dataOutput.writeUTF(this.d3o);
        dataOutput.writeUTF(this.d7o);
        dataOutput.writeUTF(this.d30o);
        dataOutput.writeUTF(this.d60o);
        dataOutput.writeUTF(this.d90o);
        dataOutput.writeUTF(this.d180o);
        dataOutput.writeUTF(this.totalo);
        dataOutput.writeUTF(this.registrationPlatformFD3);
        dataOutput.writeUTF(this.registrationPlatformFD7);
        dataOutput.writeUTF(this.registrationPlatformFD30);
        dataOutput.writeUTF(this.registrationPlatformFD60);
        dataOutput.writeUTF(this.registrationPlatformFD90);
        dataOutput.writeUTF(this.registrationPlatformFD180);
        dataOutput.writeUTF(this.registrationPlatformFTotal);
        dataOutput.writeUTF(this.applyForALloanFD3);
        dataOutput.writeUTF(this.applyForALloanFD7);
        dataOutput.writeUTF(this.applyForALloanFD30);
        dataOutput.writeUTF(this.applyForALloanFD60);
        dataOutput.writeUTF(this.applyForALloanFD90);
        dataOutput.writeUTF(this.applyForALloanFD180);
        dataOutput.writeUTF(this.applyForALloanFTotal);
        dataOutput.writeUTF(this.requestRejectionFD3);
        dataOutput.writeUTF(this.requestRejectionFD7);
        dataOutput.writeUTF(this.requestRejectionFD30);
        dataOutput.writeUTF(this.requestRejectionFD60);
        dataOutput.writeUTF(this.requestRejectionFD90);
        dataOutput.writeUTF(this.requestRejectionFD180);
        dataOutput.writeUTF(this.requestRejectionFTotal);
        dataOutput.writeUTF(this.passFD3);
        dataOutput.writeUTF(this.passFD7);
        dataOutput.writeUTF(this.passFD30);
        dataOutput.writeUTF(this.passFD60);
        dataOutput.writeUTF(this.passFD90);
        dataOutput.writeUTF(this.passFD180);
        dataOutput.writeUTF(this.passFTotal);
        dataOutput.writeUTF(this.smRelationId);
        dataOutput.writeUTF(this.behaviorType);
        dataOutput.writeUTF(this.smLendingQDId);
        dataOutput.writeUTF(this.overallRiskLevel);
        dataOutput.writeUTF(this.riskScore);
        dataOutput.writeUTF(this.strategyName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tableName = dataInput.readUTF();
        this.machineSearchIdentify = dataInput.readUTF();
        this.machineSearchSN = dataInput.readUTF();
        this.queryDataFlowSn = dataInput.readUTF();
        this.tripartitePrimaryKey = dataInput.readUTF();
        this.borrowerBorrowerId = dataInput.readUTF();
        this.borrowerOrderSn = dataInput.readUTF();
        this.borrowerLoanType = dataInput.readUTF();
        this.borrowerBorrowerMoney = dataInput.readUTF();
        this.borrowerPayWay = dataInput.readUTF();
        this.borrowerBorrowerPeriod = dataInput.readUTF();
        this.borrowerName = dataInput.readUTF();
        this.borrowerIdCard = dataInput.readUTF();
        this.borrowerBankCard = dataInput.readUTF();
        this.borrowerMobile = dataInput.readUTF();
        this.infoOrderSn = dataInput.readUTF();
        this.borrowerInfoSex = dataInput.readUTF();
        this.borrowerInfoEducation = dataInput.readUTF();
        this.borrowerInfoMarriageStatus = dataInput.readUTF();
        this.borrowerInfoContactPhone = dataInput.readUTF();
        this.borrowerInfoLoanPurpose = dataInput.readUTF();
        this.borrowerInfoGuaranteeMeasure = dataInput.readUTF();
        this.borrowerInfoIncomeSource = dataInput.readUTF();
        this.borrowerInfoCompanyName = dataInput.readUTF();
        this.borrowerInfoCompanyAddress = dataInput.readUTF();
        this.borrowerInfoCompanyPhone = dataInput.readUTF();
        this.borrowerInfoProfession = dataInput.readUTF();
        this.contactOrderSn = dataInput.readUTF();
        this.borrowerContactEmergencyContactName = dataInput.readUTF();
        this.borrowerContactEmergencyContactRelation = dataInput.readUTF();
        this.borrowerContactEmergencyContactPhone = dataInput.readUTF();
        this.numberOfEmergencyContacts = dataInput.readUTF();
        this.extraOrderSn = dataInput.readUTF();
        this.additionalInformationOfPartner = dataInput.readUTF();
        this.loanApplicationTime = dataInput.readUTF();
        this.numberOfMobileLink = dataInput.readUTF();
        this.netTime = dataInput.readUTF();
        this.activeFrequency = dataInput.readUTF();
        this.aveCommunicationCost = dataInput.readUTF();
        this.hlslQueryDataId = dataInput.readUTF();
        this.hlslQueryDataQUId = dataInput.readUTF();
        this.hlslQueryDataBorrowerId = dataInput.readUTF();
        this.hlslQueryDataUserName = dataInput.readUTF();
        this.hlslQueryDataUserIdCard = dataInput.readUTF();
        this.hlslQueryDataUserPhone = dataInput.readUTF();
        this.hlslUserBasicQUID = dataInput.readUTF();
        this.hlslUserBasicAge = dataInput.readUTF();
        this.hlslUserBasicBirthday = dataInput.readUTF();
        this.hlslUserBasicGender = dataInput.readUTF();
        this.hlslUserBasicIdCardCity = dataInput.readUTF();
        this.hlslUserBasicIdCardProvince = dataInput.readUTF();
        this.hlslUserBasicIdCardRegion = dataInput.readUTF();
        this.hlslUserBasicIdCardValidate = dataInput.readUTF();
        this.hlslUserBasicLastAppearIdcard = dataInput.readUTF();
        this.hlslUserBasicLastAppearPhone = dataInput.readUTF();
        this.hlslUserBasicPhoneCity = dataInput.readUTF();
        this.hlslUserBasicPhoneOperator = dataInput.readUTF();
        this.hlslUserBasicPhoneProvince = dataInput.readUTF();
        this.hlslUserBasicRecordIdCardDays = dataInput.readUTF();
        this.hlslUserBasicRecordPhoneDays = dataInput.readUTF();
        this.hlslUserBasicUsedIdCardsCnt = dataInput.readUTF();
        this.hlslUserBasicUsedPhonesCnt = dataInput.readUTF();
        this.hlslHistoryOrgQUId = dataInput.readUTF();
        this.hlslHistoryOrgCreditCardRepaymentCnt = dataInput.readUTF();
        this.hlslHistoryOrgOfflineCashLoanCnt = dataInput.readUTF();
        this.hlslHistoryOrgOfflineInstallmentCnt = dataInput.readUTF();
        this.hlslHistoryOrgOnlineCashLoanCnt = dataInput.readUTF();
        this.hlslHistoryOrgOnlineInstallmentCnt = dataInput.readUTF();
        this.hlslHistoryOrgOthersCnt = dataInput.readUTF();
        this.hlslHistoryOrgPaydayLoanCnt = dataInput.readUTF();
        this.hlslHistorySearchQUId = dataInput.readUTF();
        this.hlsrHistorySearchOrgCnt = dataInput.readUTF();
        this.hlslHistorySearchSearchCntRecent14Days = dataInput.readUTF();
        this.hlslHistorySearchSearchCntRecent180Days = dataInput.readUTF();
        this.hlslHistorySearchSearchCntRecent30Days = dataInput.readUTF();
        this.hlslHistorySearchSearchCntRecent60Days = dataInput.readUTF();
        this.hlslHistorySearchSearchCntRecent7Days = dataInput.readUTF();
        this.hlslHistorySearchSearchCntRecent90Days = dataInput.readUTF();
        this.hlslHistorySearchSearchCnt = dataInput.readUTF();
        this.hlslHistorySearchOrgCntRecent14Days = dataInput.readUTF();
        this.hlslHistorySearchOrgCntRecent180Days = dataInput.readUTF();
        this.hlslHistorySearchOrgCntRecent30Days = dataInput.readUTF();
        this.hlslHistorySearchOrgCntRecent60Days = dataInput.readUTF();
        this.hlslHistorySearchOrgCntRecent7Days = dataInput.readUTF();
        this.hlslHistorySearchOrgCntRecent90Days = dataInput.readUTF();
        this.paLoanQueryDataQDId = dataInput.readUTF();
        this.paLoanQueryDataId = dataInput.readUTF();
        this.paLoanQueryDataBorrowerId = dataInput.readUTF();
        this.paLoanLoanRecordQDId = dataInput.readUTF();
        this.paLoanLoanRecordId = dataInput.readUTF();
        this.paLoanClassificationId = dataInput.readUTF();
        this.paLoanClassificationRId = dataInput.readUTF();
        this.paLoanClassificationClassificationType = dataInput.readUTF();
        this.paLoanClassificationClassificationSection = dataInput.readUTF();
        this.paLoanClassificationOrgNums = dataInput.readUTF();
        this.otherM3FOrgNums = dataInput.readUTF();
        this.otherM6FOrgNums = dataInput.readUTF();
        this.otherM9FOrgNums = dataInput.readUTF();
        this.otherM12FOrgNums = dataInput.readUTF();
        this.otherM24FOrgNums = dataInput.readUTF();
        this.bankM3FOrgNums = dataInput.readUTF();
        this.bankM6FOrgNums = dataInput.readUTF();
        this.bankM9FOrgNums = dataInput.readUTF();
        this.bankM12FOrgNums = dataInput.readUTF();
        this.bankM24FOrgNums = dataInput.readUTF();
        this.bqsQueryDataQDId = dataInput.readUTF();
        this.bqsQueryDataId = dataInput.readUTF();
        this.bqsQueryDataFinalDecision = dataInput.readUTF();
        this.bqsQueryDataBorrowerId = dataInput.readUTF();
        this.bqsStrategyQDId = dataInput.readUTF();
        this.bqsStrategyId = dataInput.readUTF();
        this.bqsRuleStrategyId = dataInput.readUTF();
        this.ruleID = dataInput.readUTF();
        this.ruleID_378967 = dataInput.readUTF();
        this.ruleID_378968 = dataInput.readUTF();
        this.ruleID_378969 = dataInput.readUTF();
        this.ruleID_378970 = dataInput.readUTF();
        this.ruleID_378971 = dataInput.readUTF();
        this.ruleID_378972 = dataInput.readUTF();
        this.ruleID_378973 = dataInput.readUTF();
        this.ruleID_378974 = dataInput.readUTF();
        this.ruleID_378994 = dataInput.readUTF();
        this.ruleID_378995 = dataInput.readUTF();
        this.ruleID_379012 = dataInput.readUTF();
        this.zxtHighestRiskQUId = dataInput.readUTF();
        this.highestRiskLevelDescription = dataInput.readUTF();
        this.highestRiskRevel = dataInput.readUTF();
        this.zxtHighestRiskBorrowerId = dataInput.readUTF();
        this.smLoanQDId = dataInput.readUTF();
        this.smLoanLoanAction = dataInput.readUTF();
        this.smLoanPlatformType = dataInput.readUTF();
        this.smLoanVariable = dataInput.readUTF();
        this.d3 = dataInput.readUTF();
        this.d7 = dataInput.readUTF();
        this.d30 = dataInput.readUTF();
        this.d60 = dataInput.readUTF();
        this.d90 = dataInput.readUTF();
        this.d180 = dataInput.readUTF();
        this.total = dataInput.readUTF();
        this.d3o = dataInput.readUTF();
        this.d7o = dataInput.readUTF();
        this.d30o = dataInput.readUTF();
        this.d60o = dataInput.readUTF();
        this.d90o = dataInput.readUTF();
        this.d180o = dataInput.readUTF();
        this.totalo = dataInput.readUTF();
        this.registrationPlatformFD3 = dataInput.readUTF();
        this.registrationPlatformFD7 = dataInput.readUTF();
        this.registrationPlatformFD30 = dataInput.readUTF();
        this.registrationPlatformFD60 = dataInput.readUTF();
        this.registrationPlatformFD90 = dataInput.readUTF();
        this.registrationPlatformFD180 = dataInput.readUTF();
        this.registrationPlatformFTotal = dataInput.readUTF();
        this.applyForALloanFD3 = dataInput.readUTF();
        this.applyForALloanFD7 = dataInput.readUTF();
        this.applyForALloanFD30 = dataInput.readUTF();
        this.applyForALloanFD60 = dataInput.readUTF();
        this.applyForALloanFD90 = dataInput.readUTF();
        this.applyForALloanFD180 = dataInput.readUTF();
        this.applyForALloanFTotal = dataInput.readUTF();
        this.requestRejectionFD3 = dataInput.readUTF();
        this.requestRejectionFD7 = dataInput.readUTF();
        this.requestRejectionFD30 = dataInput.readUTF();
        this.requestRejectionFD60 = dataInput.readUTF();
        this.requestRejectionFD90 = dataInput.readUTF();
        this.requestRejectionFD180 = dataInput.readUTF();
        this.requestRejectionFTotal = dataInput.readUTF();
        this.passFD3 = dataInput.readUTF();
        this.passFD7 = dataInput.readUTF();
        this.passFD30 = dataInput.readUTF();
        this.passFD60 = dataInput.readUTF();
        this.passFD90 = dataInput.readUTF();
        this.passFD180 = dataInput.readUTF();
        this.passFTotal = dataInput.readUTF();
        this.smRelationId = dataInput.readUTF();
        this.behaviorType = dataInput.readUTF();
        this.smLendingQDId = dataInput.readUTF();
        this.overallRiskLevel = dataInput.readUTF();
        this.riskScore = dataInput.readUTF();
        this.strategyName = dataInput.readUTF();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof WideTableWritable)) {
            return false;
        }

        WideTableWritable that = (WideTableWritable) o;

        return new EqualsBuilder()
                .append(getTableName(), that.getTableName())
                .append(getMachineSearchIdentify(), that.getMachineSearchIdentify())
                .append(getMachineSearchSN(), that.getMachineSearchSN())
                .append(getQueryDataFlowSn(), that.getQueryDataFlowSn())
                .append(getTripartitePrimaryKey(), that.getTripartitePrimaryKey())
                .append(getBorrowerBorrowerId(), that.getBorrowerBorrowerId())
                .append(getBorrowerOrderSn(), that.getBorrowerOrderSn())
                .append(getBorrowerLoanType(), that.getBorrowerLoanType())
                .append(getBorrowerBorrowerMoney(), that.getBorrowerBorrowerMoney())
                .append(getBorrowerPayWay(), that.getBorrowerPayWay())
                .append(getBorrowerBorrowerPeriod(), that.getBorrowerBorrowerPeriod())
                .append(getBorrowerName(), that.getBorrowerName())
                .append(getBorrowerIdCard(), that.getBorrowerIdCard())
                .append(getBorrowerBankCard(), that.getBorrowerBankCard())
                .append(getBorrowerMobile(), that.getBorrowerMobile())
                .append(getInfoOrderSn(), that.getInfoOrderSn())
                .append(getBorrowerInfoSex(), that.getBorrowerInfoSex())
                .append(getBorrowerInfoEducation(), that.getBorrowerInfoEducation())
                .append(getBorrowerInfoMarriageStatus(), that.getBorrowerInfoMarriageStatus())
                .append(getBorrowerInfoContactPhone(), that.getBorrowerInfoContactPhone())
                .append(getBorrowerInfoLoanPurpose(), that.getBorrowerInfoLoanPurpose())
                .append(getBorrowerInfoGuaranteeMeasure(), that.getBorrowerInfoGuaranteeMeasure())
                .append(getBorrowerInfoIncomeSource(), that.getBorrowerInfoIncomeSource())
                .append(getBorrowerInfoCompanyName(), that.getBorrowerInfoCompanyName())
                .append(getBorrowerInfoCompanyAddress(), that.getBorrowerInfoCompanyAddress())
                .append(getBorrowerInfoCompanyPhone(), that.getBorrowerInfoCompanyPhone())
                .append(getBorrowerInfoProfession(), that.getBorrowerInfoProfession())
                .append(getContactOrderSn(), that.getContactOrderSn())
                .append(getBorrowerContactEmergencyContactName(), that.getBorrowerContactEmergencyContactName())
                .append(getBorrowerContactEmergencyContactRelation(), that.getBorrowerContactEmergencyContactRelation())
                .append(getBorrowerContactEmergencyContactPhone(), that.getBorrowerContactEmergencyContactPhone())
                .append(getNumberOfEmergencyContacts(), that.getNumberOfEmergencyContacts())
                .append(getExtraOrderSn(), that.getExtraOrderSn())
                .append(getAdditionalInformationOfPartner(), that.getAdditionalInformationOfPartner())
                .append(getLoanApplicationTime(), that.getLoanApplicationTime())
                .append(getNumberOfMobileLink(), that.getNumberOfMobileLink())
                .append(getNetTime(), that.getNetTime())
                .append(getActiveFrequency(), that.getActiveFrequency())
                .append(getAveCommunicationCost(), that.getAveCommunicationCost())
                .append(getHlslQueryDataId(), that.getHlslQueryDataId())
                .append(getHlslQueryDataQUId(), that.getHlslQueryDataQUId())
                .append(getHlslQueryDataBorrowerId(), that.getHlslQueryDataBorrowerId())
                .append(getHlslQueryDataUserName(), that.getHlslQueryDataUserName())
                .append(getHlslQueryDataUserIdCard(), that.getHlslQueryDataUserIdCard())
                .append(getHlslQueryDataUserPhone(), that.getHlslQueryDataUserPhone())
                .append(getHlslUserBasicQUID(), that.getHlslUserBasicQUID())
                .append(getHlslUserBasicAge(), that.getHlslUserBasicAge())
                .append(getHlslUserBasicBirthday(), that.getHlslUserBasicBirthday())
                .append(getHlslUserBasicGender(), that.getHlslUserBasicGender())
                .append(getHlslUserBasicIdCardCity(), that.getHlslUserBasicIdCardCity())
                .append(getHlslUserBasicIdCardProvince(), that.getHlslUserBasicIdCardProvince())
                .append(getHlslUserBasicIdCardRegion(), that.getHlslUserBasicIdCardRegion())
                .append(getHlslUserBasicIdCardValidate(), that.getHlslUserBasicIdCardValidate())
                .append(getHlslUserBasicLastAppearIdcard(), that.getHlslUserBasicLastAppearIdcard())
                .append(getHlslUserBasicLastAppearPhone(), that.getHlslUserBasicLastAppearPhone())
                .append(getHlslUserBasicPhoneCity(), that.getHlslUserBasicPhoneCity())
                .append(getHlslUserBasicPhoneOperator(), that.getHlslUserBasicPhoneOperator())
                .append(getHlslUserBasicPhoneProvince(), that.getHlslUserBasicPhoneProvince())
                .append(getHlslUserBasicRecordIdCardDays(), that.getHlslUserBasicRecordIdCardDays())
                .append(getHlslUserBasicRecordPhoneDays(), that.getHlslUserBasicRecordPhoneDays())
                .append(getHlslUserBasicUsedIdCardsCnt(), that.getHlslUserBasicUsedIdCardsCnt())
                .append(getHlslUserBasicUsedPhonesCnt(), that.getHlslUserBasicUsedPhonesCnt())
                .append(getHlslHistoryOrgQUId(), that.getHlslHistoryOrgQUId())
                .append(getHlslHistoryOrgCreditCardRepaymentCnt(), that.getHlslHistoryOrgCreditCardRepaymentCnt())
                .append(getHlslHistoryOrgOfflineCashLoanCnt(), that.getHlslHistoryOrgOfflineCashLoanCnt())
                .append(getHlslHistoryOrgOfflineInstallmentCnt(), that.getHlslHistoryOrgOfflineInstallmentCnt())
                .append(getHlslHistoryOrgOnlineCashLoanCnt(), that.getHlslHistoryOrgOnlineCashLoanCnt())
                .append(getHlslHistoryOrgOnlineInstallmentCnt(), that.getHlslHistoryOrgOnlineInstallmentCnt())
                .append(getHlslHistoryOrgOthersCnt(), that.getHlslHistoryOrgOthersCnt())
                .append(getHlslHistoryOrgPaydayLoanCnt(), that.getHlslHistoryOrgPaydayLoanCnt())
                .append(getHlslHistorySearchQUId(), that.getHlslHistorySearchQUId())
                .append(getHlsrHistorySearchOrgCnt(), that.getHlsrHistorySearchOrgCnt())
                .append(getHlslHistorySearchSearchCntRecent14Days(), that.getHlslHistorySearchSearchCntRecent14Days())
                .append(getHlslHistorySearchSearchCntRecent180Days(), that.getHlslHistorySearchSearchCntRecent180Days())
                .append(getHlslHistorySearchSearchCntRecent30Days(), that.getHlslHistorySearchSearchCntRecent30Days())
                .append(getHlslHistorySearchSearchCntRecent60Days(), that.getHlslHistorySearchSearchCntRecent60Days())
                .append(getHlslHistorySearchSearchCntRecent7Days(), that.getHlslHistorySearchSearchCntRecent7Days())
                .append(getHlslHistorySearchSearchCntRecent90Days(), that.getHlslHistorySearchSearchCntRecent90Days())
                .append(getHlslHistorySearchSearchCnt(), that.getHlslHistorySearchSearchCnt())
                .append(getHlslHistorySearchOrgCntRecent14Days(), that.getHlslHistorySearchOrgCntRecent14Days())
                .append(getHlslHistorySearchOrgCntRecent180Days(), that.getHlslHistorySearchOrgCntRecent180Days())
                .append(getHlslHistorySearchOrgCntRecent30Days(), that.getHlslHistorySearchOrgCntRecent30Days())
                .append(getHlslHistorySearchOrgCntRecent60Days(), that.getHlslHistorySearchOrgCntRecent60Days())
                .append(getHlslHistorySearchOrgCntRecent7Days(), that.getHlslHistorySearchOrgCntRecent7Days())
                .append(getHlslHistorySearchOrgCntRecent90Days(), that.getHlslHistorySearchOrgCntRecent90Days())
                .append(getPaLoanQueryDataQDId(), that.getPaLoanQueryDataQDId())
                .append(getPaLoanQueryDataId(), that.getPaLoanQueryDataId())
                .append(getPaLoanQueryDataBorrowerId(), that.getPaLoanQueryDataBorrowerId())
                .append(getPaLoanLoanRecordQDId(), that.getPaLoanLoanRecordQDId())
                .append(getPaLoanLoanRecordId(), that.getPaLoanLoanRecordId())
                .append(getPaLoanClassificationId(), that.getPaLoanClassificationId())
                .append(getPaLoanClassificationRId(), that.getPaLoanClassificationRId())
                .append(getPaLoanClassificationClassificationType(), that.getPaLoanClassificationClassificationType())
                .append(getPaLoanClassificationClassificationSection(), that.getPaLoanClassificationClassificationSection())
                .append(getPaLoanClassificationOrgNums(), that.getPaLoanClassificationOrgNums())
                .append(getOtherM3FOrgNums(), that.getOtherM3FOrgNums())
                .append(getOtherM6FOrgNums(), that.getOtherM6FOrgNums())
                .append(getOtherM9FOrgNums(), that.getOtherM9FOrgNums())
                .append(getOtherM12FOrgNums(), that.getOtherM12FOrgNums())
                .append(getOtherM24FOrgNums(), that.getOtherM24FOrgNums())
                .append(getBankM3FOrgNums(), that.getBankM3FOrgNums())
                .append(getBankM6FOrgNums(), that.getBankM6FOrgNums())
                .append(getBankM9FOrgNums(), that.getBankM9FOrgNums())
                .append(getBankM12FOrgNums(), that.getBankM12FOrgNums())
                .append(getBankM24FOrgNums(), that.getBankM24FOrgNums())
                .append(getBqsQueryDataQDId(), that.getBqsQueryDataQDId())
                .append(getBqsQueryDataId(), that.getBqsQueryDataId())
                .append(getBqsQueryDataFinalDecision(), that.getBqsQueryDataFinalDecision())
                .append(getBqsQueryDataBorrowerId(), that.getBqsQueryDataBorrowerId())
                .append(getBqsStrategyQDId(), that.getBqsStrategyQDId())
                .append(getBqsStrategyId(), that.getBqsStrategyId())
                .append(getBqsRuleStrategyId(), that.getBqsRuleStrategyId())
                .append(getRuleID(), that.getRuleID())
                .append(getRuleID_378967(), that.getRuleID_378967())
                .append(getRuleID_378968(), that.getRuleID_378968())
                .append(getRuleID_378969(), that.getRuleID_378969())
                .append(getRuleID_378970(), that.getRuleID_378970())
                .append(getRuleID_378971(), that.getRuleID_378971())
                .append(getRuleID_378972(), that.getRuleID_378972())
                .append(getRuleID_378973(), that.getRuleID_378973())
                .append(getRuleID_378974(), that.getRuleID_378974())
                .append(getRuleID_378994(), that.getRuleID_378994())
                .append(getRuleID_378995(), that.getRuleID_378995())
                .append(getRuleID_379012(), that.getRuleID_379012())
                .append(getZxtHighestRiskQUId(), that.getZxtHighestRiskQUId())
                .append(getHighestRiskLevelDescription(), that.getHighestRiskLevelDescription())
                .append(getHighestRiskRevel(), that.getHighestRiskRevel())
                .append(getZxtHighestRiskBorrowerId(), that.getZxtHighestRiskBorrowerId())
                .append(getSmLoanQDId(), that.getSmLoanQDId())
                .append(getSmLoanLoanAction(), that.getSmLoanLoanAction())
                .append(getSmLoanPlatformType(), that.getSmLoanPlatformType())
                .append(getSmLoanVariable(), that.getSmLoanVariable())
                .append(getD3(), that.getD3())
                .append(getD7(), that.getD7())
                .append(getD30(), that.getD30())
                .append(getD60(), that.getD60())
                .append(getD90(), that.getD90())
                .append(getD180(), that.getD180())
                .append(getTotal(), that.getTotal())
                .append(getD3o(), that.getD3o())
                .append(getD7o(), that.getD7o())
                .append(getD30o(), that.getD30o())
                .append(getD60o(), that.getD60o())
                .append(getD90o(), that.getD90o())
                .append(getD180o(), that.getD180o())
                .append(getTotalo(), that.getTotalo())
                .append(getRegistrationPlatformFD3(), that.getRegistrationPlatformFD3())
                .append(getRegistrationPlatformFD7(), that.getRegistrationPlatformFD7())
                .append(getRegistrationPlatformFD30(), that.getRegistrationPlatformFD30())
                .append(getRegistrationPlatformFD60(), that.getRegistrationPlatformFD60())
                .append(getRegistrationPlatformFD90(), that.getRegistrationPlatformFD90())
                .append(getRegistrationPlatformFD180(), that.getRegistrationPlatformFD180())
                .append(getRegistrationPlatformFTotal(), that.getRegistrationPlatformFTotal())
                .append(getApplyForALloanFD3(), that.getApplyForALloanFD3())
                .append(getApplyForALloanFD7(), that.getApplyForALloanFD7())
                .append(getApplyForALloanFD30(), that.getApplyForALloanFD30())
                .append(getApplyForALloanFD60(), that.getApplyForALloanFD60())
                .append(getApplyForALloanFD90(), that.getApplyForALloanFD90())
                .append(getApplyForALloanFD180(), that.getApplyForALloanFD180())
                .append(getApplyForALloanFTotal(), that.getApplyForALloanFTotal())
                .append(getRequestRejectionFD3(), that.getRequestRejectionFD3())
                .append(getRequestRejectionFD7(), that.getRequestRejectionFD7())
                .append(getRequestRejectionFD30(), that.getRequestRejectionFD30())
                .append(getRequestRejectionFD60(), that.getRequestRejectionFD60())
                .append(getRequestRejectionFD90(), that.getRequestRejectionFD90())
                .append(getRequestRejectionFD180(), that.getRequestRejectionFD180())
                .append(getRequestRejectionFTotal(), that.getRequestRejectionFTotal())
                .append(getPassFD3(), that.getPassFD3())
                .append(getPassFD7(), that.getPassFD7())
                .append(getPassFD30(), that.getPassFD30())
                .append(getPassFD60(), that.getPassFD60())
                .append(getPassFD90(), that.getPassFD90())
                .append(getPassFD180(), that.getPassFD180())
                .append(getPassFTotal(), that.getPassFTotal())
                .append(getSmRelationId(), that.getSmRelationId())
                .append(getBehaviorType(), that.getBehaviorType())
                .append(getSmLendingQDId(), that.getSmLendingQDId())
                .append(getOverallRiskLevel(), that.getOverallRiskLevel())
                .append(getRiskScore(), that.getRiskScore())
                .append(getStrategyName(), that.getStrategyName())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getTableName())
                .append(getMachineSearchIdentify())
                .append(getMachineSearchSN())
                .append(getQueryDataFlowSn())
                .append(getTripartitePrimaryKey())
                .append(getBorrowerBorrowerId())
                .append(getBorrowerOrderSn())
                .append(getBorrowerLoanType())
                .append(getBorrowerBorrowerMoney())
                .append(getBorrowerPayWay())
                .append(getBorrowerBorrowerPeriod())
                .append(getBorrowerName())
                .append(getBorrowerIdCard())
                .append(getBorrowerBankCard())
                .append(getBorrowerMobile())
                .append(getInfoOrderSn())
                .append(getBorrowerInfoSex())
                .append(getBorrowerInfoEducation())
                .append(getBorrowerInfoMarriageStatus())
                .append(getBorrowerInfoContactPhone())
                .append(getBorrowerInfoLoanPurpose())
                .append(getBorrowerInfoGuaranteeMeasure())
                .append(getBorrowerInfoIncomeSource())
                .append(getBorrowerInfoCompanyName())
                .append(getBorrowerInfoCompanyAddress())
                .append(getBorrowerInfoCompanyPhone())
                .append(getBorrowerInfoProfession())
                .append(getContactOrderSn())
                .append(getBorrowerContactEmergencyContactName())
                .append(getBorrowerContactEmergencyContactRelation())
                .append(getBorrowerContactEmergencyContactPhone())
                .append(getNumberOfEmergencyContacts())
                .append(getExtraOrderSn())
                .append(getAdditionalInformationOfPartner())
                .append(getLoanApplicationTime())
                .append(getNumberOfMobileLink())
                .append(getNetTime())
                .append(getActiveFrequency())
                .append(getAveCommunicationCost())
                .append(getHlslQueryDataId())
                .append(getHlslQueryDataQUId())
                .append(getHlslQueryDataBorrowerId())
                .append(getHlslQueryDataUserName())
                .append(getHlslQueryDataUserIdCard())
                .append(getHlslQueryDataUserPhone())
                .append(getHlslUserBasicQUID())
                .append(getHlslUserBasicAge())
                .append(getHlslUserBasicBirthday())
                .append(getHlslUserBasicGender())
                .append(getHlslUserBasicIdCardCity())
                .append(getHlslUserBasicIdCardProvince())
                .append(getHlslUserBasicIdCardRegion())
                .append(getHlslUserBasicIdCardValidate())
                .append(getHlslUserBasicLastAppearIdcard())
                .append(getHlslUserBasicLastAppearPhone())
                .append(getHlslUserBasicPhoneCity())
                .append(getHlslUserBasicPhoneOperator())
                .append(getHlslUserBasicPhoneProvince())
                .append(getHlslUserBasicRecordIdCardDays())
                .append(getHlslUserBasicRecordPhoneDays())
                .append(getHlslUserBasicUsedIdCardsCnt())
                .append(getHlslUserBasicUsedPhonesCnt())
                .append(getHlslHistoryOrgQUId())
                .append(getHlslHistoryOrgCreditCardRepaymentCnt())
                .append(getHlslHistoryOrgOfflineCashLoanCnt())
                .append(getHlslHistoryOrgOfflineInstallmentCnt())
                .append(getHlslHistoryOrgOnlineCashLoanCnt())
                .append(getHlslHistoryOrgOnlineInstallmentCnt())
                .append(getHlslHistoryOrgOthersCnt())
                .append(getHlslHistoryOrgPaydayLoanCnt())
                .append(getHlslHistorySearchQUId())
                .append(getHlsrHistorySearchOrgCnt())
                .append(getHlslHistorySearchSearchCntRecent14Days())
                .append(getHlslHistorySearchSearchCntRecent180Days())
                .append(getHlslHistorySearchSearchCntRecent30Days())
                .append(getHlslHistorySearchSearchCntRecent60Days())
                .append(getHlslHistorySearchSearchCntRecent7Days())
                .append(getHlslHistorySearchSearchCntRecent90Days())
                .append(getHlslHistorySearchSearchCnt())
                .append(getHlslHistorySearchOrgCntRecent14Days())
                .append(getHlslHistorySearchOrgCntRecent180Days())
                .append(getHlslHistorySearchOrgCntRecent30Days())
                .append(getHlslHistorySearchOrgCntRecent60Days())
                .append(getHlslHistorySearchOrgCntRecent7Days())
                .append(getHlslHistorySearchOrgCntRecent90Days())
                .append(getPaLoanQueryDataQDId())
                .append(getPaLoanQueryDataId())
                .append(getPaLoanQueryDataBorrowerId())
                .append(getPaLoanLoanRecordQDId())
                .append(getPaLoanLoanRecordId())
                .append(getPaLoanClassificationId())
                .append(getPaLoanClassificationRId())
                .append(getPaLoanClassificationClassificationType())
                .append(getPaLoanClassificationClassificationSection())
                .append(getPaLoanClassificationOrgNums())
                .append(getOtherM3FOrgNums())
                .append(getOtherM6FOrgNums())
                .append(getOtherM9FOrgNums())
                .append(getOtherM12FOrgNums())
                .append(getOtherM24FOrgNums())
                .append(getBankM3FOrgNums())
                .append(getBankM6FOrgNums())
                .append(getBankM9FOrgNums())
                .append(getBankM12FOrgNums())
                .append(getBankM24FOrgNums())
                .append(getBqsQueryDataQDId())
                .append(getBqsQueryDataId())
                .append(getBqsQueryDataFinalDecision())
                .append(getBqsQueryDataBorrowerId())
                .append(getBqsStrategyQDId())
                .append(getBqsStrategyId())
                .append(getBqsRuleStrategyId())
                .append(getRuleID())
                .append(getRuleID_378967())
                .append(getRuleID_378968())
                .append(getRuleID_378969())
                .append(getRuleID_378970())
                .append(getRuleID_378971())
                .append(getRuleID_378972())
                .append(getRuleID_378973())
                .append(getRuleID_378974())
                .append(getRuleID_378994())
                .append(getRuleID_378995())
                .append(getRuleID_379012())
                .append(getZxtHighestRiskQUId())
                .append(getHighestRiskLevelDescription())
                .append(getHighestRiskRevel())
                .append(getZxtHighestRiskBorrowerId())
                .append(getSmLoanQDId())
                .append(getSmLoanLoanAction())
                .append(getSmLoanPlatformType())
                .append(getSmLoanVariable())
                .append(getD3())
                .append(getD7())
                .append(getD30())
                .append(getD60())
                .append(getD90())
                .append(getD180())
                .append(getTotal())
                .append(getD3o())
                .append(getD7o())
                .append(getD30o())
                .append(getD60o())
                .append(getD90o())
                .append(getD180o())
                .append(getTotalo())
                .append(getRegistrationPlatformFD3())
                .append(getRegistrationPlatformFD7())
                .append(getRegistrationPlatformFD30())
                .append(getRegistrationPlatformFD60())
                .append(getRegistrationPlatformFD90())
                .append(getRegistrationPlatformFD180())
                .append(getRegistrationPlatformFTotal())
                .append(getApplyForALloanFD3())
                .append(getApplyForALloanFD7())
                .append(getApplyForALloanFD30())
                .append(getApplyForALloanFD60())
                .append(getApplyForALloanFD90())
                .append(getApplyForALloanFD180())
                .append(getApplyForALloanFTotal())
                .append(getRequestRejectionFD3())
                .append(getRequestRejectionFD7())
                .append(getRequestRejectionFD30())
                .append(getRequestRejectionFD60())
                .append(getRequestRejectionFD90())
                .append(getRequestRejectionFD180())
                .append(getRequestRejectionFTotal())
                .append(getPassFD3())
                .append(getPassFD7())
                .append(getPassFD30())
                .append(getPassFD60())
                .append(getPassFD90())
                .append(getPassFD180())
                .append(getPassFTotal())
                .append(getSmRelationId())
                .append(getBehaviorType())
                .append(getSmLendingQDId())
                .append(getOverallRiskLevel())
                .append(getRiskScore())
                .append(getStrategyName())
                .toHashCode();
    }


    public String toStringother() {
        final StringBuilder sb = new StringBuilder("");
        sb
                .append(tableName);
        sb.append("$|$")
                .append(machineSearchIdentify);
        sb.append("$|$")
                .append(machineSearchSN);
        sb.append("$|$")
                .append(queryDataFlowSn);
        sb.append("$|$")
                .append(tripartitePrimaryKey);
        sb.append("$|$")
                .append(borrowerBorrowerId);
        sb.append("$|$")
                .append(borrowerOrderSn);
        sb.append("$|$")
                .append(borrowerLoanType);
        sb.append("$|$")
                .append(borrowerBorrowerMoney);
        sb.append("$|$")
                .append(borrowerPayWay);
        sb.append("$|$")
                .append(borrowerBorrowerPeriod);
        sb.append("$|$")
                .append(borrowerName);
        sb.append("$|$")
                .append(borrowerIdCard);
        sb.append("$|$")
                .append(borrowerBankCard);
        sb.append("$|$")
                .append(borrowerMobile);
        sb.append("$|$")
                .append(infoOrderSn);
        sb.append("$|$")
                .append(borrowerInfoSex);
        sb.append("$|$")
                .append(borrowerInfoEducation);
        sb.append("$|$")
                .append(borrowerInfoMarriageStatus);
        sb.append("$|$")
                .append(borrowerInfoContactPhone);
        sb.append("$|$")
                .append(borrowerInfoLoanPurpose);
        sb.append("$|$")
                .append(borrowerInfoGuaranteeMeasure);
        sb.append("$|$")
                .append(borrowerInfoIncomeSource);
        sb.append("$|$")
                .append(borrowerInfoCompanyName);
        sb.append("$|$")
                .append(borrowerInfoCompanyAddress);
        sb.append("$|$")
                .append(borrowerInfoCompanyPhone);
        sb.append("$|$")
                .append(borrowerInfoProfession);
        sb.append("$|$")
                .append(contactOrderSn);
        sb.append("$|$")
                .append(borrowerContactEmergencyContactName);
        sb.append("$|$")
                .append(borrowerContactEmergencyContactRelation);
        sb.append("$|$")
                .append(borrowerContactEmergencyContactPhone);
        sb.append("$|$")
                .append(numberOfEmergencyContacts);
        sb.append("$|$")
                .append(extraOrderSn);
        sb.append("$|$")
                .append(additionalInformationOfPartner);
        sb.append("$|$")
                .append(loanApplicationTime);
        sb.append("$|$")
                .append(numberOfMobileLink);
        sb.append("$|$")
                .append(netTime);
        sb.append("$|$")
                .append(activeFrequency);
        sb.append("$|$")
                .append(aveCommunicationCost);
        sb.append("$|$")
                .append(hlslQueryDataId);
        sb.append("$|$")
                .append(hlslQueryDataQUId);
        sb.append("$|$")
                .append(hlslQueryDataBorrowerId);
        sb.append("$|$")
                .append(hlslQueryDataUserName);
        sb.append("$|$")
                .append(hlslQueryDataUserIdCard);
        sb.append("$|$")
                .append(hlslQueryDataUserPhone);
        sb.append("$|$")
                .append(hlslUserBasicQUID);
        sb.append("$|$")
                .append(hlslUserBasicAge);
        sb.append("$|$")
                .append(hlslUserBasicBirthday);
        sb.append("$|$")
                .append(hlslUserBasicGender);
        sb.append("$|$")
                .append(hlslUserBasicIdCardCity);
        sb.append("$|$")
                .append(hlslUserBasicIdCardProvince);
        sb.append("$|$")
                .append(hlslUserBasicIdCardRegion);
        sb.append("$|$")
                .append(hlslUserBasicIdCardValidate);
        sb.append("$|$")
                .append(hlslUserBasicLastAppearIdcard);
        sb.append("$|$")
                .append(hlslUserBasicLastAppearPhone);
        sb.append("$|$")
                .append(hlslUserBasicPhoneCity);
        sb.append("$|$")
                .append(hlslUserBasicPhoneOperator);
        sb.append("$|$")
                .append(hlslUserBasicPhoneProvince);
        sb.append("$|$")
                .append(hlslUserBasicRecordIdCardDays);
        sb.append("$|$")
                .append(hlslUserBasicRecordPhoneDays);
        sb.append("$|$")
                .append(hlslUserBasicUsedIdCardsCnt);
        sb.append("$|$")
                .append(hlslUserBasicUsedPhonesCnt);
        sb.append("$|$")
                .append(hlslHistoryOrgQUId);
        sb.append("$|$")
                .append(hlslHistoryOrgCreditCardRepaymentCnt);
        sb.append("$|$")
                .append(hlslHistoryOrgOfflineCashLoanCnt);
        sb.append("$|$")
                .append(hlslHistoryOrgOfflineInstallmentCnt);
        sb.append("$|$")
                .append(hlslHistoryOrgOnlineCashLoanCnt);
        sb.append("$|$")
                .append(hlslHistoryOrgOnlineInstallmentCnt);
        sb.append("$|$")
                .append(hlslHistoryOrgOthersCnt);
        sb.append("$|$")
                .append(hlslHistoryOrgPaydayLoanCnt);
        sb.append("$|$")
                .append(hlslHistorySearchQUId);
        sb.append("$|$")
                .append(hlsrHistorySearchOrgCnt);
        sb.append("$|$")
                .append(hlslHistorySearchSearchCntRecent14Days);
        sb.append("$|$")
                .append(hlslHistorySearchSearchCntRecent180Days);
        sb.append("$|$")
                .append(hlslHistorySearchSearchCntRecent30Days);
        sb.append("$|$")
                .append(hlslHistorySearchSearchCntRecent60Days);
        sb.append("$|$")
                .append(hlslHistorySearchSearchCntRecent7Days);
        sb.append("$|$")
                .append(hlslHistorySearchSearchCntRecent90Days);
        sb.append("$|$")
                .append(hlslHistorySearchSearchCnt);
        sb.append("$|$")
                .append(hlslHistorySearchOrgCntRecent14Days);
        sb.append("$|$")
                .append(hlslHistorySearchOrgCntRecent180Days);
        sb.append("$|$")
                .append(hlslHistorySearchOrgCntRecent30Days);
        sb.append("$|$")
                .append(hlslHistorySearchOrgCntRecent60Days);
        sb.append("$|$")
                .append(hlslHistorySearchOrgCntRecent7Days);
        sb.append("$|$")
                .append(hlslHistorySearchOrgCntRecent90Days);
        sb.append("$|$")
                .append(paLoanQueryDataQDId);
        sb.append("$|$")
                .append(paLoanQueryDataId);
        sb.append("$|$")
                .append(paLoanQueryDataBorrowerId);
        sb.append("$|$")
                .append(paLoanLoanRecordQDId);
        sb.append("$|$")
                .append(paLoanLoanRecordId);
        sb.append("$|$")
                .append(paLoanClassificationId);
        sb.append("$|$")
                .append(paLoanClassificationRId);
        sb.append("$|$")
                .append(paLoanClassificationClassificationType);
        sb.append("$|$")
                .append(paLoanClassificationClassificationSection);
        sb.append("$|$")
                .append(paLoanClassificationOrgNums);
        sb.append("$|$")
                .append(otherM3FOrgNums);
        sb.append("$|$")
                .append(otherM6FOrgNums);
        sb.append("$|$")
                .append(otherM9FOrgNums);
        sb.append("$|$")
                .append(otherM12FOrgNums);
        sb.append("$|$")
                .append(otherM24FOrgNums);
        sb.append("$|$")
                .append(bankM3FOrgNums);
        sb.append("$|$")
                .append(bankM6FOrgNums);
        sb.append("$|$")
                .append(bankM9FOrgNums);
        sb.append("$|$")
                .append(bankM12FOrgNums);
        sb.append("$|$")
                .append(bankM24FOrgNums);
        sb.append("$|$")
                .append(bqsQueryDataQDId);
        sb.append("$|$")
                .append(bqsQueryDataId);
        sb.append("$|$")
                .append(bqsQueryDataFinalDecision);
        sb.append("$|$")
                .append(bqsQueryDataBorrowerId);
        sb.append("$|$")
                .append(bqsStrategyQDId);
        sb.append("$|$")
                .append(bqsStrategyId);
        sb.append("$|$")
                .append(bqsRuleStrategyId);
        sb.append("$|$")
                .append(ruleID);
        sb.append("$|$")
                .append(ruleID_378967);
        sb.append("$|$")
                .append(ruleID_378968);
        sb.append("$|$")
                .append(ruleID_378969);
        sb.append("$|$")
                .append(ruleID_378970);
        sb.append("$|$")
                .append(ruleID_378971);
        sb.append("$|$")
                .append(ruleID_378972);
        sb.append("$|$")
                .append(ruleID_378973);
        sb.append("$|$")
                .append(ruleID_378974);
        sb.append("$|$")
                .append(ruleID_378994);
        sb.append("$|$")
                .append(ruleID_378995);
        sb.append("$|$")
                .append(ruleID_379012);
        sb.append("$|$")
                .append(zxtHighestRiskQUId);
        sb.append("$|$")
                .append(highestRiskLevelDescription);
        sb.append("$|$")
                .append(highestRiskRevel);
        sb.append("$|$")
                .append(zxtHighestRiskBorrowerId);
        sb.append("$|$")
                .append(smLoanQDId);
        sb.append("$|$")
                .append(smLoanLoanAction);
        sb.append("$|$")
                .append(smLoanPlatformType);
        sb.append("$|$")
                .append(smLoanVariable);
        sb.append("$|$")
                .append(d3);
        sb.append("$|$")
                .append(d7);
        sb.append("$|$")
                .append(d30);
        sb.append("$|$")
                .append(d60);
        sb.append("$|$")
                .append(d90);
        sb.append("$|$")
                .append(d180);
        sb.append("$|$")
                .append(total);
        sb.append("$|$")
                .append(d3o);
        sb.append("$|$")
                .append(d7o);
        sb.append("$|$")
                .append(d30o);
        sb.append("$|$")
                .append(d60o);
        sb.append("$|$")
                .append(d90o);
        sb.append("$|$")
                .append(d180o);
        sb.append("$|$")
                .append(totalo);
        sb.append("$|$")
                .append(registrationPlatformFD3);
        sb.append("$|$")
                .append(registrationPlatformFD7);
        sb.append("$|$")
                .append(registrationPlatformFD30);
        sb.append("$|$")
                .append(registrationPlatformFD60);
        sb.append("$|$")
                .append(registrationPlatformFD90);
        sb.append("$|$")
                .append(registrationPlatformFD180);
        sb.append("$|$")
                .append(registrationPlatformFTotal);
        sb.append("$|$")
                .append(applyForALloanFD3);
        sb.append("$|$")
                .append(applyForALloanFD7);
        sb.append("$|$")
                .append(applyForALloanFD30);
        sb.append("$|$")
                .append(applyForALloanFD60);
        sb.append("$|$")
                .append(applyForALloanFD90);
        sb.append("$|$")
                .append(applyForALloanFD180);
        sb.append("$|$")
                .append(applyForALloanFTotal);
        sb.append("$|$")
                .append(requestRejectionFD3);
        sb.append("$|$")
                .append(requestRejectionFD7);
        sb.append("$|$")
                .append(requestRejectionFD30);
        sb.append("$|$")
                .append(requestRejectionFD60);
        sb.append("$|$")
                .append(requestRejectionFD90);
        sb.append("$|$")
                .append(requestRejectionFD180);
        sb.append("$|$")
                .append(requestRejectionFTotal);
        sb.append("$|$")
                .append(passFD3);
        sb.append("$|$")
                .append(passFD7);
        sb.append("$|$")
                .append(passFD30);
        sb.append("$|$")
                .append(passFD60);
        sb.append("$|$")
                .append(passFD90);
        sb.append("$|$")
                .append(passFD180);
        sb.append("$|$")
                .append(passFTotal);
        sb.append("$|$")
                .append(smRelationId);
        sb.append("$|$")
                .append(behaviorType);
        sb.append("$|$")
                .append(smLendingQDId);
        sb.append("$|$")
                .append(overallRiskLevel);
        sb.append("$|$")
                .append(riskScore);
        sb.append("$|$")
                .append(strategyName);
        return sb.toString();
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("");
        sb.append(borrowerBorrowerId);
        sb.append(",").append(borrowerOrderSn);
        sb.append(",").append(borrowerBorrowerMoney);
        sb.append(",").append(borrowerInfoSex);
        sb.append(",").append(borrowerInfoEducation);
        sb.append(",").append(borrowerInfoMarriageStatus);
        sb.append(",").append(borrowerInfoLoanPurpose);
        sb.append(",").append(borrowerInfoIncomeSource);
        sb.append(",").append(borrowerInfoProfession);
        sb.append(",").append(loanApplicationTime);
        sb.append(",").append(numberOfMobileLink);
        sb.append(",").append(netTime);
        sb.append(",").append(activeFrequency);
        sb.append(",").append(aveCommunicationCost);
        sb.append(",").append(hlslUserBasicAge);
        sb.append(",").append(hlslUserBasicIdCardCity);
        sb.append(",").append(hlslUserBasicIdCardProvince);
        sb.append(",").append(hlslUserBasicLastAppearIdcard);
        sb.append(",").append(hlslUserBasicLastAppearPhone);
        sb.append(",").append(hlslUserBasicPhoneCity);
        sb.append(",").append(hlslUserBasicPhoneOperator);
        sb.append(",").append(hlslUserBasicPhoneProvince);
        sb.append(",").append(hlslUserBasicRecordIdCardDays);
        sb.append(",").append(hlslUserBasicRecordPhoneDays);
        sb.append(",").append(hlslUserBasicUsedIdCardsCnt);
        sb.append(",").append(hlslUserBasicUsedPhonesCnt);
        sb.append(",").append(hlslHistoryOrgCreditCardRepaymentCnt);
        sb.append(",").append(hlslHistoryOrgOfflineCashLoanCnt);
        sb.append(",").append(hlslHistoryOrgOfflineInstallmentCnt);
        sb.append(",").append(hlslHistoryOrgOnlineCashLoanCnt);
        sb.append(",").append(hlslHistoryOrgOnlineInstallmentCnt);
        sb.append(",").append(hlslHistoryOrgOthersCnt);
        sb.append(",").append(hlslHistoryOrgPaydayLoanCnt);
        sb.append(",").append(hlsrHistorySearchOrgCnt);
        sb.append(",").append(hlslHistorySearchSearchCntRecent14Days);
        sb.append(",").append(hlslHistorySearchSearchCntRecent180Days);
        sb.append(",").append(hlslHistorySearchSearchCntRecent30Days);
        sb.append(",").append(hlslHistorySearchSearchCntRecent60Days);
        sb.append(",").append(hlslHistorySearchSearchCntRecent7Days);
        sb.append(",").append(hlslHistorySearchSearchCntRecent90Days);
        sb.append(",").append(hlslHistorySearchSearchCnt);
        sb.append(",").append(hlslHistorySearchOrgCntRecent14Days);
        sb.append(",").append(hlslHistorySearchOrgCntRecent180Days);
        sb.append(",").append(hlslHistorySearchOrgCntRecent30Days);
        sb.append(",").append(hlslHistorySearchOrgCntRecent60Days);
        sb.append(",").append(hlslHistorySearchOrgCntRecent7Days);
        sb.append(",").append(hlslHistorySearchOrgCntRecent90Days);
        sb.append(",").append(otherM3FOrgNums);
        sb.append(",").append(otherM6FOrgNums);
        sb.append(",").append(otherM9FOrgNums);
        sb.append(",").append(otherM12FOrgNums);
        sb.append(",").append(otherM24FOrgNums);
        sb.append(",").append(bankM3FOrgNums);
        sb.append(",").append(bankM6FOrgNums);
        sb.append(",").append(bankM9FOrgNums);
        sb.append(",").append(bankM12FOrgNums);
        sb.append(",").append(bankM24FOrgNums);
        sb.append(",").append(bqsQueryDataFinalDecision);
        sb.append(",").append(ruleID_378967);
        sb.append(",").append(ruleID_378968);
        sb.append(",").append(ruleID_378969);
        sb.append(",").append(ruleID_378970);
        sb.append(",").append(ruleID_378971);
        sb.append(",").append(ruleID_378972);
        sb.append(",").append(ruleID_378973);
        sb.append(",").append(ruleID_378974);
        sb.append(",").append(ruleID_378994);
        sb.append(",").append(ruleID_378995);
        sb.append(",").append(ruleID_379012);
        sb.append(",").append(highestRiskLevelDescription);
        sb.append(",").append(highestRiskRevel);
        sb.append(",").append(d3o);
        sb.append(",").append(d7o);
        sb.append(",").append(d30o);
        sb.append(",").append(d60o);
        sb.append(",").append(d90o);
        sb.append(",").append(d180o);
        sb.append(",").append(totalo);
        sb.append(",").append(registrationPlatformFD3);
        sb.append(",").append(registrationPlatformFD7);
        sb.append(",").append(registrationPlatformFD30);
        sb.append(",").append(registrationPlatformFD60);
        sb.append(",").append(registrationPlatformFD90);
        sb.append(",").append(registrationPlatformFD180);
        sb.append(",").append(registrationPlatformFTotal);
        sb.append(",").append(applyForALloanFD3);
        sb.append(",").append(applyForALloanFD7);
        sb.append(",").append(applyForALloanFD30);
        sb.append(",").append(applyForALloanFD60);
        sb.append(",").append(applyForALloanFD90);
        sb.append(",").append(applyForALloanFD180);
        sb.append(",").append(applyForALloanFTotal);
        sb.append(",").append(requestRejectionFD3);
        sb.append(",").append(requestRejectionFD7);
        sb.append(",").append(requestRejectionFD30);
        sb.append(",").append(requestRejectionFD60);
        sb.append(",").append(requestRejectionFD90);
        sb.append(",").append(requestRejectionFD180);
        sb.append(",").append(requestRejectionFTotal);
        sb.append(",").append(passFD3);
        sb.append(",").append(passFD7);
        sb.append(",").append(passFD30);
        sb.append(",").append(passFD60);
        sb.append(",").append(passFD90);
        sb.append(",").append(passFD180);
        sb.append(",").append(passFTotal);
        sb.append(",").append(overallRiskLevel);
        sb.append(",").append(riskScore);
        return sb.toString();
    }

    public boolean isJson(String content) {
        try {
            JSONObject jsonStr = JSONObject.parseObject(content);
            return true;
        } catch (Exception e) {
            return false;
        }
    }


}
