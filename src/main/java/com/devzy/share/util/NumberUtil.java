package com.devzy.share.util;

public class NumberUtil {
	public static boolean isNumber(String target){
		if(target==null||"".equals(target)||"null".equals(target)){
			return false;
		}
		for(int i=0;i<target.length();i++){
			char c = target.charAt(i);
			if(!(c>='0'&&c<='9')){
				return false;
			}
		}
		return true;
	}
	public static long extractNumber(String str) {
		str = str.trim();
		String strNum = "";// 从字符串中提取数字
		if (str != null && !"".equals(str)) {
			for (int i = 0; i < str.length(); i++) {
				if (str.charAt(i) >= 48 && str.charAt(i) <= 57) {
					strNum += str.charAt(i);
				}
			}
		}
		long num = Long.parseLong(strNum);
		return num;
	}
}
