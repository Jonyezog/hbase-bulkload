package com.apache.hbase.coprocessor;

import java.util.List;

import org.apache.hadoop.hbase.regionserver.HRegion;

public class TableScanThread implements Runnable {

	private List<HRegion> regions;
	
	public TableScanThread(List<HRegion> regions){
	}
	
	public void run() {
		

	}

}
