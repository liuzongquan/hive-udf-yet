package com.yidu.hive.udf;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDF;

public class WangDianDistanceUDF extends UDF {
	public static HashMap<String,Float[]> wd2geo = new HashMap<String,Float[]>();
	public WangDianDistanceUDF(){
		// read wangdian dat from hdfs file : wangdian.dat
		String filename = "./wangdian.dat";
		try {
			@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			String line=null;
			while((line=reader.readLine())!=null){
				String[] splits=line.split("\\s+");
				if(splits.length < 3) continue;
				Float[] jing_wei = null;
				try{
					jing_wei=new Float[]{Float.parseFloat(splits[1]),Float.parseFloat(splits[2])};
				}catch (NumberFormatException e) {
//					e.printStackTrace();
					continue;
				} 
				wd2geo.put(splits[0], jing_wei);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	public String evaluate(String jing, String wei) {
		double min_distance = Double.MAX_VALUE;
		String wd_id="";
		double lat2, lng2;
		try{
		 lat2 = Double.parseDouble(wei);
		 lng2 = Double.parseDouble(jing);
		}catch(NumberFormatException nfe){
			return null;
		}
		for(String key : wd2geo.keySet()){
//			wd_id = key;
			Float[] jing_wei = wd2geo.get(key);
			double lat1 = jing_wei[1];
			double lng1 = jing_wei[0];
			double distance = this.GetDistance(lat1,lng1,lat2,lng2);
			if(distance < min_distance){
				min_distance = distance;
				wd_id = key;
			}
		}
		System.err.println(wd_id+","+min_distance);
		StringBuilder sb = new StringBuilder();
		sb.append("{\"wd_id\":\"").append(wd_id).append("\",\"distance\":\"").append(min_distance).append("\"}");
		return sb.toString();
	}
	
	/** 
	 * google maps的脚本里代码 
	 */    
	
	private static double rad(double d) 
	{ 
	     return d * Math.PI / 180.0; 
	}  

	/** 
	 * 根据两点间经纬度坐标（double值），计算两点间距离，单位为米 
	 */ 
	public double GetDistance(double lat1, double lng1, double lat2, double lng2) 
	{ 
		double EARTH_RADIUS = 6378137; 
	    double radLat1 = rad(lat1); 
	    double radLat2 = rad(lat2); 
	    double a = radLat1 - radLat2; 
	    double b = rad(lng1) - rad(lng2); 
	    double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) + 
	    Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2))); 
	    s = s * EARTH_RADIUS; 
	    s = Math.round(s * 10000) / 10000; 
	    return s; 
	} 
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
