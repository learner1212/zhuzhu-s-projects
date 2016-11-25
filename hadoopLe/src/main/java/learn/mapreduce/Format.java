package learn.mapreduce;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class Format {

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DecimalFormat decimalFormat = new DecimalFormat("00.0000");

    public static String getDateTime() {
        return simpleDateFormat.format(System.currentTimeMillis());
    }

    public static String getDecimal(double vaule) {
        return decimalFormat.format(vaule);
    }
}
