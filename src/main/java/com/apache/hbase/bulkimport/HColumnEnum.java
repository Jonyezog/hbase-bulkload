package com.apache.hbase.bulkimport;

/**
 * 定义Hbase table column family
 * @author zhangfeng
 *
 */
public enum HColumnEnum {
	SRV_COL_B("c1".getBytes());

	private final byte[] columnName;

	HColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}
