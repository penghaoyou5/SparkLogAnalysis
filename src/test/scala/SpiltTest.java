import jdk.nashorn.internal.runtime.regexp.joni.Regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpiltTest {
    final public static String TAG_REGEX = "\\%(\\w|\\%)|\\%\\{([\\w\\.-]+)\\}";

//    static String TAG_REGEX = "11";
    final public static Pattern tagPattern = Pattern.compile(TAG_REGEX);

    public static void main(String args[]){

String path = "%{dd.-}";
//path         ="";

        Matcher matcher = tagPattern.matcher(path);
//        matcher.group(2);
        System.out.println(matcher.find());
        System.out.println(matcher.group(2));

    }
}
