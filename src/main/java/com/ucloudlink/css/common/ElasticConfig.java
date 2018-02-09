package com.ucloudlink.css.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ElasticConfig {
	private static Logger logger = LogManager.getLogger(ElasticConfig.class);
	private static Properties config = null;
	
	public static Properties getInstance(String properties){
		InputStream in = null; 
		try {
			in = ClassLoader.getSystemResourceAsStream(properties);
			config = new Properties();
			config.load(in);
		} catch (IOException e) {
			logger.error("--Canal Properties read error!",e);
		}finally{
			if(in!=null){
				try {
					in.close();
				} catch (Exception e) {
					logger.error("--Canal InputStream read error!",e);
				}
			}
		}
		return config;
	}
	
	public static String getProperty(String key) throws Exception{
		if(config==null){
			config = getInstance("elastic.properties");
		}
		return config.getProperty(key);
	}
}