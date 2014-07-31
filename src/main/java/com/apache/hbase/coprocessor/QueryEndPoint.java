package com.apache.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.Cell;

import com.apache.hbase.coprocessor.generated.ServerQueryProcess;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryRequest;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * Hbase endpiont Coprocessor server端数据过滤接口实现
 * @author zhangfeng
 *
 */
public class QueryEndPoint extends ServerQueryProcess.ServiceQuery implements CoprocessorService,Coprocessor {

	private RegionCoprocessorEnvironment env;

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
		try{
			//start rowkey
			String startRowkey = addZeroForNum(request.getGzh(),10,"0") + request.getStart();
			//end rowkey
			String endRowkey = addZeroForNum(request.getGzh(),10,"9") + request.getEnd();
			String result = this.selectByRowkeyFuzzy(request.getTableName(),startRowkey,endRowkey,request);
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
	public String selectByRowkeyFuzzy(String tableName, String startRowkey,
			String endRowkey,QueryRequest request) throws IOException {
		StringBuffer buffer = new StringBuffer("");
		InternalScanner scanner = null;
		try{
			//定义数据扫描对象
			Scan scan = new Scan();
			//设置扫描的start key
			scan.setStartRow(startRowkey.getBytes());
			//设置scan的扫描范围由startRowkey开始
			Filter filter =new InclusiveStopFilter(endRowkey.getBytes());
			scan.setFilter(filter);
			//过去region的scanner对象
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMore = false;
			do {
				//获取结果
				hasMore = scanner.next(results);
				//对结果进行匹配，找出符合条件的记录
				matchRecord(results,buffer,request);
				results.clear();
			} while (hasMore);
			return buffer.toString();
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(scanner != null){
				//关闭scanner对象，防止出现region一直打开不关闭，造成regionserver宕机的情况
				scanner.close();
			}
		}
		return null;
	}
	
	/**
	 * 对查询的结果进行匹配，找出符合条件的记录
	 * @param results 查询到的结果
	 * @param buffer 结果对象
	 * @param request 查询的条件对象
	 */
	public void matchRecord(List<Cell> results,StringBuffer buffer,QueryRequest request){
		for (Cell kv : results) {
			KeyValue keyValue = (KeyValue)kv;
			boolean frRight = true;
			boolean qyRight = true;
			boolean sbbmRight = true;
			boolean czrRight = true;
			boolean wdRight = true;
			boolean sjRight = true;
			String value = new String(keyValue.getValue());
			String[] datas = value.split(",");
			//如果法人条件不为空，并且数据中的法人和查询条件中的值不一致，结果为false
			if(!isEmpty(request.getFr()) && !request.getFr().equals(datas[2])){
				frRight = false;
			}
			//如果区域条件不为空，并且数据中的区域和查询条件中的值不一致，结果为false
			if(!isEmpty(request.getQy()) && !request.getQy().equals(datas[1])){
				qyRight = false;
			}
			//如果设备编码条件不为空，并且数据中的设备编码和查询条件中的值不一致，结果为false
			if(!isEmpty(request.getSbbm()) && !request.getSbbm().equals(datas[4])){
				sbbmRight = false;
			}
			//如果操作人条件不为空，并且数据中的操作人和查询条件中的值不一致，结果为false
			if(!isEmpty(request.getCzr()) && !request.getCzr().equals(datas[5])){
				czrRight = false;
			}
			//如果网点条件不为空，并且数据中的网点和查询条件中的值不一致，结果为false
			if(!isEmpty(request.getWd()) && !request.getWd().equals(datas[3])){
				wdRight = false;
			}
			long time = Long.parseLong(datas[0]);
			
			//判断开始和结束时间
			if( request.getStart() <=time && time <= request.getEnd() ){
				sjRight = true;
			} else {
				sjRight = false;
			}
			if(sjRight && wdRight && frRight && sbbmRight && czrRight && qyRight){
				buffer.append(value);
				buffer.append("#");
			}
		}
	}
	
	/**
	 * 判断字符串是否为空
	 * @param value
	 * @return
	 */
	private boolean isEmpty(String value){
		if(value != null && !value.equals("")){
			return false;
		}
		return true;
	}	
}
