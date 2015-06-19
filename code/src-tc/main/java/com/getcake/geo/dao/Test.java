package com.getcake.geo.dao;

import java.nio.charset.Charset;
import java.util.HashMap;

public class Test {

	public static void main(String[] args) {
		
	  long num = Long.MAX_VALUE, num2 = 72057594037927935L;
	  HashMap<Byte, Integer> test;
	  
	  if (num > 72057594037927935L) {
		  
	  }
	  
	  // Long.compareUnsigned(x, y)
	  String	 tmp;
	  
	  Charset UTF8   = Charset.forName("UTF-8");
	  
	  byte b2 [] = hexStringToByteArray ("4567");
	  
	  /*
	  tmp = stringToHex ("4567");
	  
	  char c1 = '4' << 4;
	  // byte  b1 = c1 | c1;
	  
	  tmp = Integer.toBinaryString(Integer.parseInt("45", 16));
      byte b[] = tmp.getBytes();
      System.out.println (b);
	  */	  
	}
	
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
	
	/*
	static private  String hex(byte[] bytes) {
	    StringBuilder sb = new StringBuilder();
	    for (int i=0; i<bytes.length; i++) {
	        sb.append(String.format("%02X ",bytes[i]));
	    }
	    return sb.toString();
	}	

	static String stringToHex(String string) {
		  StringBuilder buf = new StringBuilder(200);
		  for (char ch: string.toCharArray()) {
		    if (buf.length() > 0)
		      buf.append(' ');
		    buf.append(String.format("%04x", (int) ch));
		  }
		  return buf.toString();
		}
	*/	
}
