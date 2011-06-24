package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.Collection;
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.Random;
import java.text.SimpleDateFormat;

import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public class OfferService implements Runnable {

  public static final Log LOG = LogFactory.getLog(OfferService.class.getName());

  long lastHeartbeat = 0;
  volatile boolean shouldRun = true;
  long lastBlockReport = 0;
  boolean resetBlockReportTime = true;
  DataNode datanode;
  DatanodeProtocol namenode;
  InetSocketAddress namenodeAddress;
  DatanodeRegistration dnRegistration = null;
  FSDatasetInterface data;
  DataNodeMetrics myMetrics;
  private static final Random R = new Random();
  private int backlogSize; // if we accumulate this many blockReceived, then it is time
                           // to send a block report. Otherwise the receivedBlockList
                           // might exceed our Heap size.

  /**
   * A data structure to store Block and delHints together
   */
  private static class BlockInfo extends Block {
    String delHints;
    BlockInfo(Block blk, String delHints) {
      super(blk);
      this.delHints = delHints;
    }
  }

  private TreeSet<BlockInfo> receivedBlockList = new TreeSet<BlockInfo>();
  private TreeSet<BlockInfo> retryBlockList = new TreeSet<BlockInfo>();
  private long lastBlockReceivedFailed = 0;

  /**
   * Offer service to the specified namenode
   */
  public OfferService(DataNode node,
                      DatanodeProtocol namenode, InetSocketAddress namenodeAddress) {
    this.datanode = node;
    this.namenode = namenode;
    this.namenodeAddress = namenodeAddress;
    dnRegistration = node.dnRegistration;
    data = node.data;
    myMetrics = node.myMetrics;
    scheduleBlockReport(node.initialBlockReportDelay);
    backlogSize = node.getConf().getInt("dfs.datanode.blockreceived.backlog", 10000);
  }

    public void stop() {
        shouldRun = false;
    }

    public void run() {
        while (shouldRun) {
            try {
                offerService();
            } catch (Exception e) {
                LOG.error("OfferService encountered exception "
                        + StringUtils.stringifyException(e));
            }
        }
    }


      /**
       * Main loop for the DataNode.  Runs until shutdown,
       * forever calling remote NameNode functions.
       */
      public void offerService() throws Exception {

        LOG.info("using BLOCKREPORT_INTERVAL of " + datanode.blockReportInterval + "msec" +
           " Initial delay: " + datanode.initialBlockReportDelay + "msec");

        //
        // Now loop for a long time....
        //
        while (shouldRun) {
          try {

              // If we are falling behind in confirming blockReceived to NN, then
              // we clear the backlog and schedule a block report. This scenario
              // is likely to arise if one of the NN is down for an extended period.
              if (receivedBlockList.size() + retryBlockList.size() > backlogSize) {
                LOG.warn("The backlog of blocks to be confirmed has exceeded the " +
                         " configured maximum of " + backlogSize +
                         " records. Cleaning up and scheduling a block report.");
                synchronized(receivedBlockList) {
                  receivedBlockList.clear();
                  //FIXME  what should we do with delhints
                  retryBlockList.clear();
                }
                scheduleBlockReport(0);
              }


            long startTime = datanode.now();

            //
            // Every so often, send heartbeat or block-report
            //

            if (startTime - lastHeartbeat > datanode.heartBeatInterval) {
              //
              // All heartbeat messages include following info:
              // -- Datanode name
              // -- data transfer port
              // -- Total capacity
              // -- Bytes remaining
              //
              lastHeartbeat = startTime;
              DatanodeCommand[] cmds = namenode.sendHeartbeat(dnRegistration,
                                                           data.getCapacity(),
                                                           data.getDfsUsed(),
                                                           data.getRemaining(),
                                                           datanode.xmitsInProgress.get(),
                                                           datanode.getXceiverCount());
              myMetrics.heartbeats.inc(datanode.now() - startTime);
              //LOG.info("Just sent heartbeat, with name " + localName);
              if (!processCommand(cmds))
                continue;
            }

            reportReceivedBlocks();

            DatanodeCommand cmd = blockReport();
            processCommand(cmd);

            //
            // There is no work to do;  sleep until hearbeat timer elapses,
            // or work arrives, and then iterate again.
            //
            long waitTime = datanode.heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
            synchronized(receivedBlockList) {
              if (waitTime > 0 && receivedBlockList.size() == 0) {
                try {
                  receivedBlockList.wait(waitTime);
                } catch (InterruptedException ie) {
                }
              }
            } // synchronized
          } catch(RemoteException re) {
            String reClass = re.getClassName();
            if (UnregisteredNodeException.class.getName().equals(reClass) ||
                DisallowedDatanodeException.class.getName().equals(reClass) ||
                IncorrectVersionException.class.getName().equals(reClass)) {
              LOG.warn("DataNode is shutting down: " +
                       StringUtils.stringifyException(re));
              datanode.shutdown();
              return;
            }
            LOG.warn(StringUtils.stringifyException(re));
          } catch (IOException e) {
            LOG.warn(StringUtils.stringifyException(e));
          }
        } // while (shouldRun)
      } // offerService


      /**
       * Process an array of datanode commands
       *
       * @param cmds an array of datanode commands
       * @return true if further processing may be required or false otherwise.
       */
      private boolean processCommand(DatanodeCommand[] cmds) {
        if (cmds != null) {
          for (DatanodeCommand cmd : cmds) {
            try {
              if (processCommand(cmd) == false) {
                return false;
              }
            } catch (IOException ioe) {
              LOG.warn("Error processing datanode Command", ioe);
            }
          }
        }
        return true;
      }

        /**
         *
         * @param cmd
         * @return true if further processing may be required or false otherwise.
         * @throws IOException
         */
      private boolean processCommand(DatanodeCommand cmd) throws IOException {
        if (cmd == null)
          return true;
        final BlockCommand bcmd = cmd instanceof BlockCommand? (BlockCommand)cmd: null;

        switch(cmd.getAction()) {
        case DatanodeProtocol.DNA_TRANSFER:
          // Send a copy of a block to another datanode
          datanode.transferBlocks(bcmd.getBlocks(), bcmd.getTargets());
          myMetrics.blocksReplicated.inc(bcmd.getBlocks().length);
          break;
        case DatanodeProtocol.DNA_INVALIDATE:
          //
          // Some local block(s) are obsolete and can be
          // safely garbage-collected.
          //
          Block toDelete[] = bcmd.getBlocks();
          try {
            if (datanode.blockScanner != null) {
              datanode.blockScanner.deleteBlocks(toDelete);
            }
            data.invalidate(toDelete);
            datanode.removeReceivedBlocks(toDelete);
          } catch(IOException e) {
            datanode.checkDiskError();
            throw e;
          }
          myMetrics.blocksRemoved.inc(toDelete.length);
          break;
        case DatanodeProtocol.DNA_SHUTDOWN:
          // shut down the data node
          datanode.shutdown();
          return false;
        case DatanodeProtocol.DNA_REGISTER:
          // namenode requested a registration - at start or if NN lost contact
          LOG.info("DatanodeCommand action: DNA_REGISTER");
          if (shouldRun) {
            datanode.register(namenode, namenodeAddress);
            scheduleBlockReport(0);
          }
          break;
        case DatanodeProtocol.DNA_FINALIZE:
          datanode.getStorage().finalizeUpgrade();
          break;
        case UpgradeCommand.UC_ACTION_START_UPGRADE:
          // start distributed upgrade here
          processDistributedUpgradeCommand((UpgradeCommand)cmd);
          break;
        case DatanodeProtocol.DNA_RECOVERBLOCK:
          datanode.recoverBlocks(((BlockRecoveryCommand)cmd).getRecoveringBlocks());
          break;
        case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
          LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
          if (datanode.isAccessTokenEnabled) {
            datanode.accessTokenHandler.setKeys(((KeyUpdateCommand) cmd).getExportedKeys());
          }
          break;
        default:
          LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
        }
        return true;
      }

      /**
       * This methods  arranges for the data node to send the block report at the next heartbeat.
       */
      public void scheduleBlockReport(long delay) {
        if (delay > 0) { // send BR after random delay
          lastBlockReport = System.currentTimeMillis()
                                - ( datanode.blockReportInterval - R.nextInt((int)(delay)));
        } else { // send at next heartbeat
          lastBlockReport = lastHeartbeat - datanode.blockReportInterval;
        }
        resetBlockReportTime = true; // reset future BRs for randomness
      }

      /**
       * Report received blocks and delete hints to the Namenode
       * @throws IOException
       */
      private void reportReceivedBlocks() throws IOException {
        // check if there are newly received blocks
        BlockInfo [] blockArray=null;
        int numBlocks = 0;
        synchronized(receivedBlockList) {
            // retry previously failed blocks every few seconds
          if (lastBlockReceivedFailed + 10000 < datanode.now()) {
            for (BlockInfo blk : retryBlockList) {
              receivedBlockList.add(blk);
            }
            retryBlockList.clear();
          }
          numBlocks = receivedBlockList.size();
          if (numBlocks > 0) {
            blockArray = receivedBlockList.toArray(new BlockInfo[numBlocks]);
          }
        }
        if (blockArray != null) {
          String[] delHintArray = new String[numBlocks];
          Block[]  blist = new Block[numBlocks];
          for (int i = 0; i < numBlocks; i++) {
            delHintArray[i] = blockArray[i].delHints;
            blist[i] = new Block(blockArray[i]);
          }
          Block[] failed = namenode.blockReceivedNew(dnRegistration, blist,
                                                       delHintArray);
          synchronized (receivedBlockList) {
            // Blocks that do not belong to an Inode are saved for retransmisions
            for (int i = 0; i < failed.length; i++) {
              BlockInfo info = null;
              for(int j = 0; j < blockArray.length; j++) {
                if (blockArray[j].equals(failed[i])) {
                  info = blockArray[j];
                  break;
                }
              }
              if (info == null) {
                LOG.warn("BlockReceived failed for block " + failed[i] +
                         " but it is not in our request list.");
              } else if (receivedBlockList.contains(info)) {
                 // Insert into retry list only if the block was not deleted
                 // on this datanode. That is why we have to recheck if the
                 // block still exists in receivedBlockList.
                 LOG.info("Block " + info + " does not belong to any file " +
                          "on namenode " + namenodeAddress + " Retry later.");
                 retryBlockList.add(info);
                 lastBlockReceivedFailed = datanode.now();
              } else {
                 LOG.info("Block " + info + " does not belong to any file " +
                          "on namenode " + namenodeAddress +
                          " but will not be retried.");
              }
            }
            for (int i = 0; i < blockArray.length; i++) {
              receivedBlockList.remove(blockArray[i]);
            }
          }
        }

      }

      /**
       * Report the list blocks to the Namenode
       * @throws IOException
       */
      private DatanodeCommand blockReport() throws IOException {
        // send block report
        DatanodeCommand cmd = null;
        long startTime = datanode.now();
        if (startTime - lastBlockReport > datanode.blockReportInterval) {
          //
          // Send latest block report if timer has expired.
          // Get back a list of local block(s) that are obsolete
          // and can be safely GC'ed.
          //
          long brStartTime = datanode.now();
          BlockListAsLongs bReport = data.getBlockReport();

          cmd = namenode.blockReport(dnRegistration, bReport.getBlockListAsLongs());
          long brTime = datanode.now() - brStartTime;
          myMetrics.blockReports.inc(brTime);
          LOG.info("BlockReport of " + bReport.getNumberOfBlocks() +
              " blocks got processed in " + brTime + " msecs");
          //
          // If we have sent the first block report, then wait a random
          // time before we start the periodic block reports.
          //
          if (resetBlockReportTime) {
            lastBlockReport = startTime - R.nextInt((int)(datanode.blockReportInterval));
            resetBlockReportTime = false;
          } else {
            /* say the last block report was at 8:20:14. The current report
             * should have started around 9:20:14 (default 1 hour interval).
             * If current time is :
             *   1) normal like 9:20:18, next report should be at 10:20:14
             *   2) unexpected like 11:35:43, next report should be at 12:20:14
             */
            lastBlockReport += (datanode.now() - lastBlockReport) /
                datanode.blockReportInterval * datanode.blockReportInterval;
          }
        }
        return cmd;
      }

      /*
       * Informing the name node could take a long long time! Should we wait
       * till namenode is informed before responding with success to the
       * client? For now we don't.
       */
    protected void notifyNamenodeReceivedBlock(Block block, String delHint) {
        if (block == null || delHint == null) {
            throw new IllegalArgumentException(block == null ? "Block is null"
                    : "delHint is null");
        }
        synchronized (receivedBlockList) {
            receivedBlockList.add(new BlockInfo(block, delHint));
            receivedBlockList.notifyAll();
        }

    }

    private void processDistributedUpgradeCommand(UpgradeCommand comm)
            throws IOException {
        assert datanode.upgradeManager != null : "DataNode.upgradeManager is null.";
        datanode.upgradeManager.processUpgradeCommand(comm);
}


    /**
     * Remove blocks from blockReceived queues
     */
    void removeReceivedBlocks(Block[] removeList) {
        synchronized (receivedBlockList) {
            for (int i = 0; i < removeList.length; i++) {
                if (receivedBlockList.remove(removeList[i])) {
                    LOG.info("Block deletion command deleted from receivedBlockList "
                            + removeList[i]);
                } else if (retryBlockList.remove(removeList[i])) {
                    LOG.info("Block deletion command deleted from retryBlockList "
                            + removeList[i]);
                } else {
                    LOG.info("Block deletion command did not find block in "
                            + "pending blockReceived. " + removeList[i]);
                }
            }
        }
    }
}
