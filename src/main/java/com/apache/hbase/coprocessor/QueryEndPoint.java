package com.apache.hbase.coprocessor;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

import com.apache.hbase.coprocessor.generated.ServerQueryProcess;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryRequest;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class QueryEndPoint extends ServerQueryProcess.ServiceQuery implements CoprocessorService,Coprocessor {
	private static final Log LOG = LogFactory.getLog(QueryEndPoint.class);

	private RegionCoprocessorEnvironment env;

	private HBaseAdmin hbaseadmin;

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}		
	}

	public void stop(CoprocessorEnvironment env) throws IOException {		
	}

	public Service getService() {
		return this;
	}

	@Override
	public void query(RpcController controller, QueryRequest request,
			RpcCallback<QueryResponse> done) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", request.getQuorums());
		config.set("hbase.zookeeper.property.clientPort", request.getZkPort()+"");
		config.set("zookeeper.znode.parent", request.getZnodeParent());
		try{
			String startRowkey = addZeroForNum(request.getGzh(),10,"0") + request.getStart();
			String endRowkey = addZeroForNum(request.getGzh(),10,"9") + request.getEnd();
			String result = selectByRowkeyRange(request.getTableName(),startRowkey,endRowkey,config,request);
			QueryResponse resp = QueryResponse.newBuilder().setRetWord(ByteString.copyFromUtf8(result.toString())).build();
			done.run(resp);
		}catch(Exception e){
			e.printStackTrace();
		} 
	}
	
	/**
	 * 自动补充字符串到指定长度
	 * @param str
	 * @param strLength
	 * @param ch
	 * @return
	 */
	private String addZeroForNum(String str, int strLength,String ch) {
	     int strLen = str.length();
	     StringBuffer sb = null;
	     while (strLen < strLength) {
	           sb = new StringBuffer();
	           sb.append(str).append(ch);
	           str = sb.toString();
	           strLen = str.length();
	     }
	     return str;
	 }
	
	
	/**
	 * 根据Rowkey的范围进行查找
	 * @param tableName
	 * @param startRowkey
	 * @param endRowkey
	 * @param conf
	 * @param request
	 * @return
	 * @throws IOException
	 */
	public String selectByRowkeyRange(String tableName, String startRowkey,
			String endRowkey,Configuration conf,QueryRequest request) throws IOException {
		HTableInterface table = null;
		StringBuffer buffer = new StringBuffer();
		try{
			hbaseadmin = new HBaseAdmin(conf);
			//如果表不存在
			if(!hbaseadmin.tableExists(tableName)){
				LOG.info(tableName + " table not found!");
				return null;
			} else {
				
				table = new HTable(conf, tableName);
				Scan scan = new Scan();
				scan.setStartRow(startRowkey.getBytes());
				//设置scan的扫描范围由startRowkey开始
				Filter filter =new InclusiveStopFilter(endRowkey.getBytes());
				scan.setFilter(filter);
				//设置scan扫描到endRowkey停止，因为setStopRow是开区间，InclusiveStopFilter设置的是闭区间
				ResultScanner rs = table.getScanner(scan);
				int count = 0;
				for (Result r : rs) {
					boolean isRight = true;
					String value = new String(r.getValue("cf".getBytes(), "c1".getBytes()));
					LOG.info("table ["+ tableName +"] query result value  :" + value);
					String[] datas = value.split(",");
					//如果法人条件不为空，并且数据中的法人和查询条件中的值不一致，结果为false
					if(!isEmpty(request.getFr()) && !request.getFr().equals(datas[2])){
						isRight = false;
					}
					//如果区域条件不为空，并且数据中的区域和查询条件中的值不一致，结果为false
					if(!isEmpty(request.getQy()) && !request.getQy().equals(datas[1])){
						isRight = false;
					}
					//如果设备编码条件不为空，并且数据中的设备编码和查询条件中的值不一致，结果为false
					if(!isEmpty(request.getSbbm()) && !request.getSbbm().equals(datas[4])){
						isRight = false;
					}
					//如果操作人条件不为空，并且数据中的操作人和查询条件中的值不一致，结果为false
					if(!isEmpty(request.getCzr()) && !request.getCzr().equals(datas[5])){
						isRight = false;
					}
					//如果网点条件不为空，并且数据中的网点和查询条件中的值不一致，结果为false
					if(!isEmpty(request.getWd()) && !request.getWd().equals(datas[3])){
						isRight = false;
					}
					long time = Long.parseLong(datas[0]);
					
					//判断开始和结束时间
					if( request.getStart() <=time && time <= request.getEnd() ){
						isRight = true;
					} else {
						isRight = false;
					}
					LOG.info("table ["+ tableName +"] query status  :" + isRight);
					if(isRight){
						LOG.info("table ["+ tableName +"] query value  :" + value);
						buffer.append(value);
						buffer.append("#");
						count ++;
					}
				}
				LOG.info("table ["+ tableName +"] query record count :" + count);
				return buffer.toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}	finally{
			if(table != null){
				table.close();
			}
		}
		return null;
	}
	
	private boolean isEmpty(String value){
		if(value != null && !value.equals("")){
			return false;
		}
		return true;
	}	
}
