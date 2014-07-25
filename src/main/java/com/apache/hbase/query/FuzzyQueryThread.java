package com.apache.hbase.query;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.log4j.Logger;

import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryRequest;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryResponse;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.ServiceQuery;
import com.google.protobuf.ByteString;

/**
 * 查询线程
 * 
 * @author zhangfeng
 * 
 */
public class FuzzyQueryThread implements Runnable {

	private static final Logger LOG = Logger.getLogger(FuzzyQueryThread.class);

	private static Configuration conf = null;

	private QueryStatusManager manager;

	private String tableName;

	private List<String> results;

	private QueryObject query;

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private String quorums;

	private int port;

	private String znodeParent;

	private HTable table;


	public FuzzyQueryThread(QueryStatusManager manager, String tableName,
			List<String> results, QueryObject query,final String quorums, final int port, final String znodeParent) {
		this.manager = manager;
		this.tableName = tableName;
		this.results = results;
		this.query = query;
		this.quorums = quorums;
		this.port = port;
		this.znodeParent = znodeParent;
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", this.quorums);
		conf.set("hbase.zookeeper.property.clientPort", this.port+"");
		conf.set("zookeeper.znode.parent", "/" +this.znodeParent);
	}

	public void run() {
		try {
			long starttime = format.parse(query.getStart()).getTime() / 1000;
			long endtime = format.parse(query.getEnd()).getTime() / 1000;
			final QueryRequest req = QueryRequest.newBuilder()
					.setStart(starttime)
					.setEnd(endtime)
					.setGzh(query.getGzh())
					.setQuorums(this.quorums)
					.setTableName(this.tableName)
					.setFr(query.getFr())
					.setQy(query.getQy())
					.setSbbm(query.getSbbm())
					.setCzr(query.getCzr())
					.setWd(query.getWd())
					.setZkPort(this.port)
					.setZnodeParent("/" + this.znodeParent)
					.build();
			table = new HTable(conf, tableName);
			Map<byte[], ByteString> re = table.coprocessorService(
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
			for (ByteString str : re.values()) {
				String results = str.toStringUtf8();
				if(results != null && !results.equals("")){
					results = results.substring(0,results.lastIndexOf("#"));
					String[] datas = results.split("#");
					if (datas != null) {
						for(String rec : datas){
							if(!this.results.contains(rec)){
								this.results.add(rec);
							}
						}
					}
					LOG.info("table ["+ tableName +"] query record count :" + datas.length);
				}
			}
			// 设置线程的查询状态为完成
			this.manager.setStatus(tableName, true);
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}


}
