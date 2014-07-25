package com.apache.hbase.query;

import java.io.Serializable;

/**
 * 查询条件对象
 * @author zhangfeng
 *
 */
public class QueryObject implements Serializable{

	private static final long serialVersionUID = -6787096990820816374L;
	
	//冠字号
	private String gzh;
	
	//法人
	private String fr;
	
	//网点
	private String wd;
	
	//设备编码
	private String sbbm;
	
	//操作人
	private String czr;
	
	//开始时间
	private String start;

	//结束时间
	private String end;
	
	//区域
	private String qy;

	public String getGzh() {
		return gzh;
	}

	public void setGzh(String gzh) {
		this.gzh = gzh;
	}

	public String getFr() {
		return fr;
	}

	public void setFr(String fr) {
		this.fr = fr;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	public String getQy() {
		return qy;
	}

	public void setQy(String qy) {
		this.qy = qy;
	}

	public String getWd() {
		return wd;
	}

	public void setWd(String wd) {
		this.wd = wd;
	}

	public String getSbbm() {
		return sbbm;
	}

	public void setSbbm(String sbbm) {
		this.sbbm = sbbm;
	}

	public String getCzr() {
		return czr;
	}

	public void setCzr(String czr) {
		this.czr = czr;
	}

}
