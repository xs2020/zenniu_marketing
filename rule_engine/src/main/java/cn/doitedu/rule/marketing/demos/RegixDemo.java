package cn.doitedu.rule.marketing.demos;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegixDemo {

    public static void main(String[] args) {

        System.out.println(regixMatch());
    }

    public static int regixMatch(){
        String pattern = "AB.*?F";
        String str = "BABAABBFCFABAACF";

        Pattern r = Pattern.compile(pattern);
        Matcher matcher = r.matcher(str);
        int count = 0;
        while(matcher.find()){
            count++;
            System.out.println(matcher.group());
        }
       // System.out.println(matcher);



        return count;
    }
}
