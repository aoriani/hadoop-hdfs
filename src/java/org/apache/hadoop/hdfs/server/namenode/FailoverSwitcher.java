package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FailoverProtocol;
import org.apache.hadoop.ipc.RPC;

public class FailoverSwitcher {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		InetSocketAddress addr = new InetSocketAddress("127.0.0.1",4444);
		FailoverProtocol failover = (FailoverProtocol) RPC.waitForProxy(FailoverProtocol.class, FailoverProtocol.versionID, addr, conf);
		failover.switchToActive();
	}

}
