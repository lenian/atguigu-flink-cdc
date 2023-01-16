package com.atguigu.oracle;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * 现场本地配置参数，系统启动时加载
 * @author K.Zhu
 * 2016-10-17
 */
public final class LocalFileConfigParam {
	private static LocalFileConfigParam localFileConfigParam = null;
	private Properties p = new Properties();

	/**
	 * 禁止new对象使用
	 */
	private LocalFileConfigParam(){
		try {
			p.load(LocalFileConfigParam.class.getResourceAsStream("/conf.properties"));
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
	
	/**
	 * 禁止new对象使用
	 */
	private LocalFileConfigParam(String filePath){
		loadParam(filePath);
	}
	
	/**
	 * 根据key值获取配置信息
	 * @param key
	 * @return
	 */
	public String getConfigValue(String key){
		return p.getProperty(key);
	}
	
	/**
	 * 获取系统变量唯一对象句柄
	 * @return
	 */
	public static LocalFileConfigParam getInstance(){
		if(localFileConfigParam == null){
			synchronized(LocalFileConfigParam.class){
				if(localFileConfigParam == null){
					localFileConfigParam = new LocalFileConfigParam();
				}
			}
		}
		return localFileConfigParam;
	}
	
	/**
	 * 加载配置文件对全局变量赋值
	 * @param filePath
	 */
	public void loadParam(String filePath) {
		try {
			p.load(LocalFileConfigParam.class.getResourceAsStream(filePath));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String toString(){
		String value = "";
		for(Object key : p.keySet()){
			value = value + key + ":" + p.get(key)  + "\n";
		}
		return value;
	}

    public static String getPropertiesString(String key, String defaultValue) {
        String value = LocalFileConfigParam.getInstance().getConfigValue(key);
        return StringUtils.isBlank(value) ? defaultValue : value;
    }

    public static Integer getPropertiesInt(String key, Integer defaultValue) {
        String value = LocalFileConfigParam.getInstance().getConfigValue(key);
        return StringUtils.isEmpty(value) ? defaultValue : Integer.parseInt(value);
    }
}
