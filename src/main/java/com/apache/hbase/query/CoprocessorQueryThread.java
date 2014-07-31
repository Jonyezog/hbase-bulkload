package com.apache.hbase.query;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryRequest;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryResponse;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.ServiceQuery;
import com.google.protobuf.ByteString;

/**
 * 基于Coprocessor查询操作的线程
 * @author zhangfeng
 *
 */
public class CoprocessorQueryThread implements Runnable {

	//查询状态管理器
	private QueryStatusManager manager;
	
	//要查询的表名，可能是多个，每个表名之间使用英文的逗号隔开，例如:GSN_20140712,FSN_20140809
	private String tableName;

	//查询对象
	private QueryObject query;
	
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	//hbase的配置对象
	private Configuration conf ;
	
	//region server的名称
	private String serverName;
	
	//coprocessor执行回调函数所在的表
	private String tn ;
	
	public CoprocessorQueryThread(String tn,Configuration conf,QueryStatusManager manager, String tableName,
			QueryObject query,String serverName) {
		this.manager = manager;
		this.query = query;
		this.tableName = tableName;
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
							//执行查询
							instance.query(controller, req, rpccall);
							//获取查询返回的结果
							QueryResponse resp = rpccall.get();
							return resp.getRetWord();
						}
					});
			//对返回结果去重
			for (ByteString str : res.values()) {
				String results = str.toStringUtf8();
				if(results != null && !results.equals("")){
					//多条结果之间server端是使用#分割的，所以这里使用#对返回的结果进行拆分
					results = results.substring(0,results.lastIndexOf("#"));
					String[] datas = results.split("#");
					if (datas != null) {
						for(String rec : datas){
							//将数据添加到结果队列中
							manager.getResults().add(rec);
						}
					}
				}
			}
			//设置这个线程的查询状态为完成
			manager.setStatus(serverName, true);
		}catch(Exception e){
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}finally{
			if(table != null){
				try {
					//关闭table对象，防止出现table一直占用rpc连接不释放的问题
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
