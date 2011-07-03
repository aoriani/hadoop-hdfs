package org.apache.hadoop.hdfs.server.namenode.failover.transactions;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ExistsTransaction extends Transaction<Stat> {

	private Watcher watcher;

	public ExistsTransaction(ZooKeeper conn, String nodePath, Watcher watcher) {
		super(conn, nodePath);
		this.watcher = watcher;
	}

	@Override
	protected void trasactionBody() throws KeeperException,
			InterruptedException {
		result = zooConn.exists(path, watcher);
	}

	@Override
	public String toString() {
		return "ExistsTransaction [result=" + result + ", path=" + path
				+ ", zooConn=" + zooConn + "]";
	}

}
