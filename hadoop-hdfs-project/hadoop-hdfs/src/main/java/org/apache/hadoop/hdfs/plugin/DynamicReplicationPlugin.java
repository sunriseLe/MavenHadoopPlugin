package org.apache.hadoop.hdfs.plugin;

import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.EnumSet;

class DynamicReplicationPlugin {
    static void allocateReplication(String src, DFSClient dfsClient) throws IOException {
        HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(src);
        // If block number is less than 1, do not adjust
        if (hdfsFileStatus.getLen() / hdfsFileStatus.getBlockSize() <= 1) {
            return;
        }
        // Set adjustment range
        final short ONE_YEAR_REPLICATION_NUM = (short) (dfsClient.getDefaultReplication() + 1);
        final short ONE_MONTH_REPLICATION_NUM = (short) (ONE_YEAR_REPLICATION_NUM + 1);
        final short ONE_WEEK_REPLICATION_NUM = (short) (ONE_MONTH_REPLICATION_NUM + 1);
        final short ONE_DAY_REPLICATION_NUM = (short) (ONE_WEEK_REPLICATION_NUM + 1);
        // Change replication
        final long ONE_DAY = 24 * 60 * 60 * 1000;
        final long ONE_WEEK = ONE_DAY * 7;
        final long ONE_MONTH = ONE_DAY * 30;
        final long ONE_YEAR = ONE_DAY * 365;
        long currentTime = new Date().getTime();
        if (dfsClient.getXAttr(src, "user.averageAccessTime") == null) {
            dfsClient.setXAttr(src, "user.averageAccessTime", ByteBuffer.allocate(8).putLong(currentTime).array(), EnumSet.of(XAttrSetFlag.CREATE));
            dfsClient.setReplication(src, ONE_DAY_REPLICATION_NUM);
        } else {
            long averageAccessTime = ByteBuffer.wrap(dfsClient.getXAttr(src, "user.averageAccessTime")).getLong();
            long newAverageAccessTime = (long) (averageAccessTime * 0.3 + currentTime * 0.7);
            dfsClient.setXAttr(src, "user.averageAccessTime", ByteBuffer.allocate(8).putLong(newAverageAccessTime).array(), EnumSet.of(XAttrSetFlag.REPLACE));
            if (currentTime - newAverageAccessTime < ONE_DAY) {
                dfsClient.setReplication(src, ONE_DAY_REPLICATION_NUM);
            } else if (currentTime - newAverageAccessTime < ONE_WEEK) {
                dfsClient.setReplication(src, ONE_WEEK_REPLICATION_NUM);
            } else if (currentTime - newAverageAccessTime < ONE_MONTH) {
                dfsClient.setReplication(src, ONE_MONTH_REPLICATION_NUM);
            } else if (currentTime - newAverageAccessTime < ONE_YEAR) {
                dfsClient.setReplication(src, ONE_YEAR_REPLICATION_NUM);
            } else {
                dfsClient.setReplication(src, dfsClient.getDefaultReplication());
            }
        }
    }
}
