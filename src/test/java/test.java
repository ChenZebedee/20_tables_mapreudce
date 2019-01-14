import java.util.ArrayList;
import java.util.List;

/**
 * Created by shaodi.chen on 2018/10/18.
 */
public class test {
    public static void main(String[] args) {
        String str1 ="t_borrower_info$|$1181632$|$SY180724142956680209$|$2635115$|$廖*思$|$1$|$43************4326$|$http://wecash-resources.oss.aliyuncs.com/i****************************$|$http://wecash-resources.oss.aliyuncs.com/id*****************************$|$2$|$6$|$2$|$130****7999$|$NULL$|$3$|$null$|$null$|$null$|$NULL$|$NULL$|$0.00$|$1$|$null$|$NULL$|$null$|$null$|$null$|$NULL$|$null$|$NULL$|$4$|$NULL$|$NULL$|$2018-07-24 14:51:38$|$2018-07-24 14:51:38";
        String str2 = "\\$\\|\\$";
        String[] str3 = str1.split(str2,-1);

        List<String> list = new ArrayList<>();
        list.add("dfjkldsajfkl");
        list.add("fjklwekjkfl");
        System.out.println(list.indexOf("fjklwekjkfl"));
    }
}
