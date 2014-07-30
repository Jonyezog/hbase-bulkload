package com.apache.hbase.query;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryRequest;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryResponse;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.ServiceQuery;
import com.google.protobuf.ByteString;

public class CoprocessorQueryThread implements Runnable {

	private QueryStatusManager manager;

	private String tableName;

//	private List<String> results;

	private QueryObject query;
	
	private HRegionInfo region;
	
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private Configuration conf ;
	
	private String serverName;
	
	private String tn ;
	
	public CoprocessorQueryThread(String tn,Configuration conf,QueryStatusManager manager, String tableName,
			QueryObject query,HRegionInfo region,String serverName) {
		this.manager = manager;
		this.query = query;
//		this.results = results;
		this.tableName = tableName;
		this.region = region;
		this.conf = conf;
		this.serverName = serverName;
		this.tn = tn;
	}
	
	
	public void run() {
		HTable table = null;
		try {
			table = new HTable(conf, tn);
			long starttime = format.parse(query.getStart()).getTime() / 1000;
			long endtime = format.parse(query.getEnd()).getTime() / 1000;
			//构造查询条件
			final QueryRequest req = QueryRequest.newBuilder()
					.setStart(starttime).setEnd(endtime)
					.setGzh(query.getGzh())
					.setTableName(this.tableName).setFr(query.getFr())
					.setQy(query.getQy()).setSbbm(query.getSbbm())
					.setCzr(query.getCzr()).setWd(query.getWd())
					.build();
			//执行coprocessor查询
			Map<byte[], ByteString> res = table.coprocessorService(
					ServiceQuery.class, null, null,
					new Batch.Call<ServiceQuery, ByteString>() {
						public ByteString call(ServiceQuery instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<QueryResponse> rpccall = new BlockingRpcCallback<QueryResponse>();
							instance.query(controller, req, rpccall);
							QueryResponse resp = rpccall.get();
							return resp.getRetWord();
						}
					});
			//对返回结果去重
			for (ByteString str : res.values()) {
				String results = str.toStringUtf8();
				if(results != null && !results.equals("")){
					results = results.substring(0,results.lastIndexOf("#"));
					String[] datas = results.split("#");
					if (datas != null) {
						for(String rec : datas){
							manager.getResults().add(rec);
						}
					}
				}
			}
			manager.setStatus(serverName, true);
		}catch(Exception e){
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}finally{
			if(table != null){
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
