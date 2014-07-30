package com.apache.hbase.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 多线程查询状态管理
 * 
 * @author zhangfeng
 *
 */
public class QueryStatusManager {
	
	private Map<String,Object> status = Collections.synchronizedMap(new HashMap<String,Object>()) ;
	
	private List<String> results = Collections.synchronizedList(new ArrayList<String>());

	public synchronized void add(String record){
		if(!this.results.contains(record)){
			this.results.add(record);
		}
	}
	
	public synchronized List<String> getResults(){
		return this.results;
	}
	
	public synchronized void setStatus(String key,Object value){
		status.put(key, value);
	}
	
	public synchronized boolean isCompleted(){
		if(status.containsValue(false)){
			return false;
		}
		return true;
	}
	
	public synchronized void clear(){
		this.status.clear();
		this.results.clear();
	}
}
