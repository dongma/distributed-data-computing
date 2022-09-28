package hadoop.apache.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 定义Hive Udf函数：zodiac(date)，依据date直接计算出星座
 *
 * @author Sam Ma
 * @date 2022/09/28
 */
@Description(name = "zodiac", value = "_FUNC_(date) - from the input date string or separate " +
        "month and day arguments, returns the sign of the Zodiac.",
        extended = "Example: \n " +
                " > SELECT _FUNC_(date_string) FROM src;\n " +
                " > SELECT _FUNC_(month, day) FROM src;"
)
public class UDFZodiacSign extends UDF {

    private SimpleDateFormat df;

    public UDFZodiacSign() {
        df = new SimpleDateFormat("MM-dd-yyyy");
    }

    /**
     * UDF#evaluate()函数，对于每行输入，都会调用此函数，然后将处理的结果值返回给Hive
     * @param bday
     * @return
     */
    public String evaluate(Date bday) {
        return this.evaluate(bday.getMonth() + 1, bday.getDate());
    }

    public String evaluate(String bday) {
        Date date = null;
        try {
            date = df.parse(bday);
        } catch (ParseException e) {
            return null;
        }
        return this.evaluate(date.getMonth() + 1, date.getDate());
    }

    public String evaluate(Integer month, Integer day) {
        if (month == 1) {
            if (day < 20) {
                return "Capricorn";
            } else {
                return "Aquarius";
            }
        }
        if (month == 2) {
            if (day < 19) {
                return "Aquarius";
            } else {
                return "Pisces";
            }
        }
        return null;
    }

}
