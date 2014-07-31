package com.apache.hbase.query;

/**
 * 表记录对象
 * @author zhangfeng
 *
 */
public class TableRecord {

	//总记录数
	private String totalRecord;
	
	//无效记录数
	private String invalRecord;
	
	//错误记录数
	private String errorRecord;

	public String getTotalRecord() {
		return totalRecord;
	}

	public void setTotalRecord(String totalRecord) {
		this.totalRecord = totalRecord;
	}

	public String getInvalRecord() {
		return invalRecord;
	}

	public void setInvalRecord(String invalRecord) {
		this.invalRecord = invalRecord;
	}

	public String getErrorRecord() {
		return errorRecord;
	}

	public void setErrorRecord(String errorRecord) {
		this.errorRecord = errorRecord;
	}
	
	
}
