package com.yidu.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Date2WeekUDF extends UDF {

	
	
	public String evaluate(String strDate){
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");// 定义日期格式  
		format.setLenient(false);
        Date date = null;  
        try {  
            date = format.parse(strDate);// 将字符串转换为日期  
        } catch (ParseException e) {  
            System.err.println("输入的日期格式不合理！");  
        }
        return getWeek(date);
	}
	
	//根据日期取得星期几  
    public static String getWeek(Date date){   
        SimpleDateFormat sdf = new SimpleDateFormat("EEEE");  
        String week = sdf.format(date);  
        return week;  
    }
	
	public static void main(String[] args) {
		Date2WeekUDF udf = new Date2WeekUDF();
		System.out.println(udf.evaluate("2016-09-22"));
	}

}
