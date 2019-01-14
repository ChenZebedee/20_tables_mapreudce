package com.mnw.info;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author bo.chao
 */
@Getter
@Setter
@Accessors(chain = true)
public class TpBorrower implements Writable {

    //表名
    private String tableName = "null";

    //t_3rdapi_query_data.f_id
    private String queryDataId = "null";

    //t_3rdapi_query_data.f_query_flow_sn
    private String queryFlowSn = "null";

    //borrowerBorrowerId
    private String borrowerId = "null";

//    private Borrower borrower = new Borrower();
//
//    private BorrowerContact borrowerContact = new BorrowerContact();
//
//    private BorrowerExtra borrowerExtra = new BorrowerExtra();
//
//    private BorrowerInfo borrowerInfo = new BorrowerInfo();

    private Hlsl hlsl = new Hlsl();

//    private Zxt zxt = new Zxt();

    public void setTpBorrower(String[] param) {
        this.tableName = param[0];
        this.queryDataId = param[1];
        this.queryFlowSn = param[2];
        this.borrowerId = param[3];
        this.hlsl.setHlsl(param[4], param[5], param[6], param[7], param[8], param[9], param[10], param[11], param[12],
                param[13], param[14], param[15], param[16], param[17], param[18], param[19], param[20], param[21], param[22],
                param[23], param[24], param[25], param[26], param[27], param[28], param[29], param[30], param[31], param[32], param[33],
                param[34], param[35], param[36], param[37], param[38], param[39], param[40], param[41], param[42], param[43], param[44]);
//        this.zxt.setZxt(param[45], param[46], param[47]);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tableName);
        dataOutput.writeUTF(queryDataId);
        dataOutput.writeUTF(queryFlowSn);
        dataOutput.writeUTF(borrowerId);
//        this.borrower.write(dataOutput);
//        this.borrowerContact.write(dataOutput);
//        this.borrowerExtra.write(dataOutput);
//        this.borrowerInfo.write(dataOutput);
        this.hlsl.write(dataOutput);
//        this.zxt.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tableName = dataInput.readUTF();
        this.queryDataId = dataInput.readUTF();
        this.queryFlowSn = dataInput.readUTF();
        this.borrowerId = dataInput.readUTF();
//        this.borrower.readFields(dataInput);
//        this.borrowerContact.readFields(dataInput);
//        this.borrowerExtra.readFields(dataInput);
//        this.borrowerInfo.readFields(dataInput);
        this.hlsl.readFields(dataInput);
//        this.zxt.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TpBorrower that = (TpBorrower) o;

        if (getTableName() != null ? !getTableName().equals(that.getTableName()) : that.getTableName() != null) {
            return false;
        }
        if (getQueryDataId() != null ? !getQueryDataId().equals(that.getQueryDataId()) : that.getQueryDataId() != null) {
            return false;
        }
        if (getQueryFlowSn() != null ? !getQueryFlowSn().equals(that.getQueryFlowSn()) : that.getQueryFlowSn() != null) {
            return false;
        }
        if (getBorrowerId() != null ? !getBorrowerId().equals(that.getBorrowerId()) : that.getBorrowerId() != null) {
            return false;
        }
        return getHlsl() != null ? !getHlsl().equals(that.getHlsl()) : that.getHlsl() != null;
//        return getZxt() != null ? getZxt().equals(that.getZxt()) : that.getZxt() == null;
    }

    @Override
    public int hashCode() {
        int result = getTableName() != null ? getTableName().hashCode() : 0;
        result = 31 * result + (getQueryDataId() != null ? getQueryDataId().hashCode() : 0);
        result = 31 * result + (getQueryFlowSn() != null ? getQueryFlowSn().hashCode() : 0);
        result = 31 * result + (getBorrowerId() != null ? getBorrowerId().hashCode() : 0);
        result = 31 * result + (getHlsl() != null ? getHlsl().hashCode() : 0);
//        result = 31 * result + (getZxt() != null ? getZxt().hashCode() : 0);
        return result;
    }

    public String toOtherString() {
        StringBuffer sb = new StringBuffer();
        sb.append(this.tableName).append("$|$")
                .append(this.queryDataId).append("$|$")
                .append(this.queryFlowSn).append("$|$")
                .append(this.borrowerId).append("$|$")
                .append(this.hlsl.toOtherString())/*.append("$|$")*/
                /*.append(this.zxt.toOtherString())*/;
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
//        sb.append(this.borrower.toString())
//            .append(",").append(this.borrowerContact.toString())
//            .append(",").append(this.borrowerExtra.toString())
//            .append(",").append(this.borrowerInfo.toString())
        sb.append(this.tableName)
                .append(",").append(this.queryDataId)
                .append(",").append(this.queryFlowSn)
                .append(",").append(this.borrowerId)
                .append(",").append(this.hlsl.toString())
                /*.append(",").append(this.zxt.toString())*/;
        return sb.toString();
    }

}
