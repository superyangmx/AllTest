package com.neunn.sparkSqlProcess;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
	public static void main(String[]  args){
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		System.out.println(ft.format(new Date()));
	}
    
}
