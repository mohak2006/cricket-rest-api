package com.bfm.acs.crazycricket.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Validate {
	public static Date dateTime(String str, String pattern){
		DateFormat format = new SimpleDateFormat(pattern);
		try {
			Date date = format.parse(str);
			return date;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static Date dateTime(String str){
		DateFormat format = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date = format.parse(str);
			return date;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
}
