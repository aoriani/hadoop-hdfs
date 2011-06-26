package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface FailoverProtocol extends VersionedProtocol {
	public static final long versionID = 60L;

	public void switchToActive() throws IOException;
}
