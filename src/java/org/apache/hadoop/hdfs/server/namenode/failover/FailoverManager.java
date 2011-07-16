package org.apache.hadoop.hdfs.server.namenode.failover;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.failover.transactions.CreateNodeTransaction;
import org.apache.hadoop.hdfs.server.namenode.failover.transactions.ExistsTransaction;
import org.apache.hadoop.hdfs.server.namenode.failover.transactions.SetDataTransaction;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class FailoverManager implements Watcher{

    private static final Logger LOG = Logger.getLogger(FailoverManager.class);

    private static final String NAMENODE_GROUP_PATH = "/namenode";
    private static final String PRIMARY_NAMENODE_PATH = NAMENODE_GROUP_PATH +"/primary";
    private static final String BACKUP_NAMENODE_PATH =  NAMENODE_GROUP_PATH +"/backup";

    private static final int ZOOKEEPER_SESSION_TIMEOUT = 500;

    private ZooKeeper zooConnection;
    private final String zooConnString;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private final NameNode namenode;
    private final NameNodeMetrics metrics;



    public FailoverManager(NameNode namenode, NameNodeMetrics metrics, String zookeeperConnString){
        this.namenode = namenode;
        this.metrics = metrics;
        this.zooConnString = zookeeperConnString;
    }


    private void connect() throws InterruptedException {
        try {
            zooConnection = new ZooKeeper(zooConnString,ZOOKEEPER_SESSION_TIMEOUT,this);
        } catch (IOException e) {
           LOG.fatal("Could not connect to Zookeeper Ensemble",e);
           //Stop namenode since it will not be able to join the group
           namenode.stop();
        }

        //We are in the process of connecting. Wait it to finish
        connectedSignal.await();
    }

    private String inetSockAddr2String(InetSocketAddress addr){
        return addr.getAddress().getHostAddress() + ":" + addr.getPort();
    }


    private void createGroupNode() throws KeeperException, InterruptedException{
        try{
            (new CreateNodeTransaction(zooConnection,NAMENODE_GROUP_PATH,null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)).invoke();
        } catch(KeeperException.NodeExistsException kenee){
            //Do nothing, it is okay if it already exist
        }
    }

    private void setupForPrimaryNamenode() throws KeeperException, InterruptedException, IOException{
        //Create our member node
        (new CreateNodeTransaction(zooConnection,PRIMARY_NAMENODE_PATH,null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)).invoke();
        //Point the current namenode to us
        byte[] data = null;
        try {
            data  = inetSockAddr2String(namenode.getRpcAddress()).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOG.fatal("Problem with enconding", e);
            throw new IOException(e.getMessage());
        }
        (new SetDataTransaction(zooConnection,NAMENODE_GROUP_PATH,data,-1)).invoke();
    }

    private void setupForBackupNamenode() throws KeeperException, InterruptedException, IOException{
        //Create our member node
        (new CreateNodeTransaction(zooConnection,BACKUP_NAMENODE_PATH,null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)).invoke();
        //Watch the primary node
        Stat result = (new ExistsTransaction(zooConnection,PRIMARY_NAMENODE_PATH, new PrimaryNodeWatcher())).invoke();
        if(result == null){
            throw new IOException("Node for primary namenode does not exist");
        }

    }

    public void register(){
        try {
            //connect to zookeeper
            connect();
            createGroupNode();
            if (namenode.getRole() == NamenodeRole.ACTIVE){
                setupForPrimaryNamenode();
            }else if (namenode.getRole() == NamenodeRole.BACKUP){
                setupForBackupNamenode();
            }
        } catch (KeeperException e) {
            namenode.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            namenode.stop();
        }
    }

    @Override
    public void process(WatchedEvent event) {
           switch (event.getState()) {
            case SyncConnected:
                LOG.info("Connected to ZooKeeper");
                connectedSignal.countDown();
                break;
            case Expired:
                LOG.fatal("ZooKeeper Session expired."+
                        " Commiting suicide since others believe I am dead");
                if (zooConnection != null)
                    try {
                        zooConnection.close();
                    } catch (InterruptedException e) {
                        LOG.error("Got interrupted when closing after expiration");
                    }
                //Stop namenode since the world think it is down
                namenode.stop();
            }
    }


    public void shutdown(){
        if(zooConnection != null){
            try {
                zooConnection.close();
            } catch (InterruptedException e) {
                LOG.warn("Connection to ZooKeeper closed with error", e);
            }
        }
    }



    private class PrimaryNodeWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {

            switch(event.getState()){

                case SyncConnected:
                    switch (event.getType()) {
                        case NodeDeleted:
                            doFailoverProcedures();
                            break;

                        default:
                            Stat result = null;
                            try {
                                result = (new ExistsTransaction(zooConnection,
                                        PRIMARY_NAMENODE_PATH, new PrimaryNodeWatcher()))
                                        .invoke();
                                if (result == null) {
                                    // Node no longer exist
                                    doFailoverProcedures();
                                }
                            } catch (KeeperException e) {
                                LOG.fatal("Some error setting the watcher", e);
                                //Safest approach is to die
                                namenode.stop();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                    }
                 break;

                case Expired:
                    namenode.stop();
                break;
            }

        }

        private void doFailoverProcedures() {

            long start = System.currentTimeMillis();

            try {
                // Primary Namenode died. Do failover
                namenode.doFailover();
                //update the server info
                byte[] data = null;
                try {
                    data  = inetSockAddr2String(namenode.getRpcAddress()).getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    LOG.fatal("Problem with enconding", e);
                    throw new IOException(e.getMessage());
                }
                (new SetDataTransaction(zooConnection,NAMENODE_GROUP_PATH,data,-1)).invoke();
            } catch (IOException e1) {
                namenode.stop();
            } catch (KeeperException e1) {
                namenode.stop();
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }

            long finish = System.currentTimeMillis();
            metrics.failoverTime.set((int)(finish-start));
        }

    }

}
