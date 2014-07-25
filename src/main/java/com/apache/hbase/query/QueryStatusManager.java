package com.apache.hbase.query;

import java.util.HashMap;
import java.util.Map;

/**
 * 多线程查询状态管理
 * 
 * @author zhangfeng
 *
 */
public class QueryStatusManager {
private Map<String,Object> status = new HashMap<String,Object>();
	
	
	public void setStatus(String key,Object value){
		status.put(key, value);
	}
	
	public boolean isCompleted(){
		if(status.containsValue(false)){
			return false;
		}
		return true;
	}
}
