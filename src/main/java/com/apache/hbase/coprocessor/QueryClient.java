package com.apache.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.conf.Configuration;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryRequest;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.QueryResponse;
import com.apache.hbase.coprocessor.generated.ServerQueryProcess.ServiceQuery;

public class QueryClient {
	public static void main(String args[]) {
		System.out.println("begin.....");
		long begin_time = System.currentTimeMillis();
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "n11,n2,n10");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("zookeeper.znode.parent", "/hbase-unsecure");
		final QueryRequest req = QueryRequest.newBuilder()
				.setStart(1404144147)
				.setEnd(1404576269)
				.setGzh("XVZQ")
//				.setQuorums("n11,n2,n10")
				.setTableName("FSN_20140711")
				.setFr("28")
				.setQy("00")
//				.setZkPort(2181)
//				.setZnodeParent("/hbase-unsecure")
				.build();
		QueryResponse resp = null;
		try {
			HTable table = new HTable(config, "FSN_20140711");
			Map<byte[], ByteString> re = table.coprocessorService(
					ServiceQuery.class, null, null,
					new Batch.Call<ServiceQuery, ByteString>() {
						public ByteString call(ServiceQuery instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<QueryResponse> rpccall = new BlockingRpcCallback<QueryResponse>();
							instance.query(controller, req, rpccall);
							QueryResponse resp = rpccall.get();
							System.out.println("resp:==========="
									+ resp.getRetWord().toStringUtf8());
							return resp.getRetWord();
						}
					});
//			for(ByteString str : re.values()){
//				System.out.println("=========== "+str.toStringUtf8());
//			}
			List<String> list = new ArrayList<String>();
			for (ByteString str : re.values()) {
				String results = str.toStringUtf8();
				System.out.println("====" + results);
				String[] datas = results.split("#");
				if (datas != null) {
					System.out.println("====" + datas.length);
					list.addAll(Arrays.asList(datas));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

}
