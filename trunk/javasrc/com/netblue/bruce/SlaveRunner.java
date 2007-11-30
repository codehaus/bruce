/*
 * Bruce - A PostgreSQL Database Replication System
 *
 * Portions Copyright (c) 2007, Connexus Corporation
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL CONNEXUS CORPORATION BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
 * PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF CONNEXUS CORPORATION HAS BEEN ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * CONNEXUS CORPORATION SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS"
 * BASIS, AND CONNEXUS CORPORATION HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
*/
package com.netblue.bruce;


import com.netblue.bruce.cluster.*;
import org.apache.commons.dbcp.*;
import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import static java.text.MessageFormat.format;

import java.sql.*;
import javax.sql.DataSource;
import java.util.*;

/**
 * Responsible for obtaining {@link com.netblue.bruce.Snapshot}s from the <code>SnapshotCache</code>
 *
 * @author lanceball
 * @version $Id$
 */
public class SlaveRunner implements Runnable {
    public SlaveRunner(final DataSource masterDataSource, final Cluster cluster, final Node node) 
	throws SQLException, InstantiationException {
	logger.debug("SlaveRunner()");
	this.node = node;
	this.cluster = cluster;
	this.masterDataSource = masterDataSource;
	this.properties = new BruceProperties();
	this.unavailableSleepTime = properties.getIntProperty(NEXT_SNAPSHOT_UNAVAILABLE_SLEEP_KEY, 
							      NEXT_SNAPSHOT_UNAVAILABLE_SLEEP_DEFAULT);
	this.availableSleepTime = properties.getIntProperty(NEXT_SNAPSHOT_AVAILABLE_SLEEP_KEY, 
							    NEXT_SNAPSHOT_AVAILABLE_SLEEP_DEFAULT);
	// slaveDataSource
	this.slaveDataSource = new BasicDataSource();
        this.slaveDataSource.setDriverClassName(properties.getProperty("bruce.jdbcDriverName", 
								       "org.postgresql.Driver"));
	this.slaveDataSource.setValidationQuery(properties.getProperty("bruce.poolQuery", "select now()"));
	this.slaveDataSource.setUrl(node.getUri());
        this.slaveDataSource.setAccessToUnderlyingConnectionAllowed(true);

	// Obtain last processed snapshot
	Connection c = this.slaveDataSource.getConnection();
	try {
	    PreparedStatement ps = c.prepareStatement(selectLastSnapshotQuery);
	    ps.setLong(1,cluster.getId());
	    ResultSet rs = ps.executeQuery();
	    if (rs.next()) {
		logger.debug("got last snapshot");
		this.lastProcessedSnapshot = 
		    new Snapshot(rs.getLong("master_id"),
				 new TransactionID(rs.getLong("master_min_xaction")),
				 new TransactionID(rs.getLong("master_max_xaction")),
				 rs.getString("master_outstanding_xactions"));
		logger.debug(lastProcessedSnapshot);
	    } else {
		logger.debug("throwing");
		throw new InstantiationException("Unable to obtain slave snapshot status. "+
						 "Please ensure that this slave on "+
						 this.node.getUri()+" has been properly initialized.");
	    }
	} finally { c.close(); }
	
	// Spit out a little info about us
	logger.info("Replicating node: "+this.node.getName()+" at "+this.node.getUri());
    }

    public void run() {
	logger.debug("run()");
	LogSwitchHelper lsh = new LogSwitchHelper(properties,slaveDataSource,cluster.getId());
	while (!shutdownRequested) {
	    try {
		lsh.doSwitch();
	    } catch (SQLException e) {
		logger.warn("SQLException during log switch, continuing",e);
	    }
	    Snapshot nextSnapshot = getNextSnapshot();
	    logger.trace("nextSnapshot: "+nextSnapshot);
	    try {
		if (nextSnapshot != null) {
		    processSnapshot(nextSnapshot);
		    Thread.sleep(availableSleepTime);
		} else {
		    Thread.sleep(unavailableSleepTime);
		}
	    } catch (InterruptedException e) {
		logger.error("Slave Interrupted",e);
		shutdownRequested=true;
	    }
	}
	try {
	    slaveDataSource.close();
	} catch (SQLException e) {} // Probably already closed.
	logger.info(node.getName()+" shutdown complete.");
    }

    public synchronized void shutdown() {
	shutdownRequested = true;
    }

    /**
     * Gets the next snapshot from the master database. Will return null if no next snapshot
     * available.
     *
     * Public so we can test this method from junit. Otherwise, probably could be private.
     *
     * @return the next Snapshot when it becomes available
     */
    public Snapshot getNextSnapshot() {
        logger.trace("Getting next snapshot");
	Snapshot retVal = null;
	try {
	    Connection c = masterDataSource.getConnection();
	    try { // Make sure the connection we just got gets closed
		PreparedStatement ps = c.prepareStatement(format(nextSnapshotQuery,cluster.getId().toString()));
		ps.setLong(1,lastProcessedSnapshot.getId());
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
		    retVal = new Snapshot(rs.getLong("id"),
					  new TransactionID(rs.getLong("min_xaction")),
					  new TransactionID(rs.getLong("max_xaction")),
					  rs.getString("outstanding_xactions"));
		}
	    } finally {
		c.close();
	    }
	} catch (SQLException e) {
	    logger.info("Can not obtain next Snapshot due to SQLException. continuing but returning null",e);
	}
	return retVal;
    }

    /**
     * Updates the slave node with transactions from <code>snapshot</code> and sets this node's status table with the
     * latest snapshot status - atomically.
     *
     * @param snapshot the <code>Snapshot</code> to process
     */
    protected void processSnapshot(final Snapshot snapshot) {
        logger.trace("Processing next snapshot: " + snapshot);
	try {
	    Connection c = slaveDataSource.getConnection();
	    try {
		// Use server side prepated statements
		PGConnection pgc = (PGConnection) ((DelegatingConnection) c).getInnermostDelegate();
		pgc.setPrepareThreshold(1);
		//
		c.setAutoCommit(false);
		c.setSavepoint();
		if (snapshot == null) {
		    logger.trace("Latest Master snapshot is null. Can not process snapshot.");
		} else {
		    collectAllChangesForTransaction(c,snapshot);
		    applyAllChangesForTransaction(c,snapshot);
		    updateSnapshotStatus(c,snapshot);
		}
		c.commit();
		this.lastProcessedSnapshot = snapshot;
	    } catch (SQLException e) {
		logger.error("Cannot commit last processed snapshot.", e);
		try {
		    if (c != null) {
			c.rollback();
		    }
		} catch (SQLException e1) {
		    logger.error("Unable to rollback last processed snapshot transaction.", e);
		}
	    } finally {
		c.close();
	    }
	} catch (SQLException e) {
	    logger.error("Unable to obtain database connection",e);
	}
    }
    
    private void collectAllChangesForTransaction(Connection slaveC, Snapshot s) throws SQLException {
	String clusterID = cluster.getId().toString();
	Connection masterC = masterDataSource.getConnection();
	try {
	    logger.trace("collectAllChangesForTransaction("+slaveC+","+s+")");
	    masterC.setAutoCommit(false);
	    // Create temp table on slave to hold transaction we are going to apply
	    Statement slaveS = slaveC.createStatement();
	    slaveS.execute(format(createTempTable,clusterID));
	    PreparedStatement masterPS = 
		masterC.prepareStatement(format(getOutstandingTransactionsQuery,clusterID));
	    masterPS.setFetchSize(50);
	    masterPS.setLong(1,lastProcessedSnapshot.getMinXid().getLong());
	    masterPS.setLong(2,s.getMaxXid().getLong());
	    ResultSet masterRS = masterPS.executeQuery();
	    PreparedStatement insertTempPS =
		slaveC.prepareStatement(insertTempTable);
	    logger.trace("Populating temporary table with transactions to apply");
	    while (masterRS.next()) {
		TransactionID tid = new TransactionID(masterRS.getLong("xaction"));
		if (lastProcessedSnapshot.transactionIDGE(tid) &&
		    s.transactionIDLT(tid)) {
		    insertTempPS.setLong(1,masterRS.getLong("rowid"));
		    insertTempPS.setLong(2,masterRS.getLong("xaction"));
		    insertTempPS.setString(3,masterRS.getString("cmdtype"));
		    insertTempPS.setString(4,masterRS.getString("tabname"));
		    insertTempPS.setString(5,masterRS.getString("info"));
		    insertTempPS.execute();
		}
	    }
	    logger.trace("Populating slave snapshot/transaction logs with data from master");
	    ResultSet slaveRS = slaveS.executeQuery(format(determineLatestLogQuery,clusterID));
	    if (slaveRS.next()) {
		String logID = slaveRS.getString("id");
		PreparedStatement ps = masterC.prepareStatement(format(getMasterSnapshotLogQuery,clusterID));
		ps.setLong(1,lastProcessedSnapshot.getId());
		ps.setLong(2,s.getId());
		ResultSet snapshotsRS = ps.executeQuery();
		while (snapshotsRS.next()) {
		    PreparedStatement popSlavePS = 
			slaveC.prepareStatement(format(populateSlaveSnapshotLogQuery,clusterID,logID));
		    popSlavePS.setLong(1,snapshotsRS.getLong("id"));
		    popSlavePS.setLong(2,snapshotsRS.getLong("min_xaction"));
		    popSlavePS.setLong(3,snapshotsRS.getLong("max_xaction"));
		    popSlavePS.setString(4,snapshotsRS.getString("outstanding_xactions"));
		    popSlavePS.execute();
		}
		slaveS.execute(format(populateSlaveTransactonLogQuery,clusterID,logID));
	    } else {
		logger.error("unable to determine current log number. Continuing anyways.");
	    }
	    logger.trace("Remove from temp table any transactions for tables we dont replicate on this slave");
	    slaveS.execute(deleteUnreplicatedTransactionsQuery);
	} finally { masterC.close(); }
    }

    private void applyAllChangesForTransaction(Connection c, Snapshot s) throws SQLException {
	Statement slaveS = c.createStatement();
	slaveS.execute(daemonModeQuery);
	slaveS.execute(applyTransactionsQuery);
	slaveS.execute(normalModeQuery);
	slaveS.execute(dropTempTable);
    }

    private void updateSnapshotStatus(Connection c, Snapshot s) throws SQLException {
	PreparedStatement ps = c.prepareStatement(updateLastSnapshotQuery);
        ps.setLong(1, getCurrentTransactionId(c));
        ps.setLong(2, s.getId());
        ps.setLong(3, new Long(s.getMinXid().toString()));
        ps.setLong(4, new Long(s.getMaxXid().toString()));
        ps.setString(5, s.getInFlight());
        ps.setLong(6, cluster.getId());
        ps.execute();
    }

    /**
     * Helper method to get the transaction ID of the currently executing transaction.
     *
     * @return The transaction ID of the currently executing transaction
     *
     * @throws SQLException
     */
    private long getCurrentTransactionId(Connection c) throws SQLException {
	Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(slaveTransactionIdQuery);
        if (rs.next()) {
	    long xid = rs.getLong("transaction");
	    rs.close();
	    return xid;
	} else {
	    logger.error("Unable to determine current transactionID");
	    return -1L;
	}
    }

    private static final Logger logger = Logger.getLogger(SlaveRunner.class);
    private Node node;
    private Cluster cluster;
    private DataSource masterDataSource;
    private BasicDataSource slaveDataSource;
    private BruceProperties properties;
    private int unavailableSleepTime;
    private int availableSleepTime;
    private Snapshot lastProcessedSnapshot;
    private boolean shutdownRequested = false;

    // How long to wait if a 'next' snapshot is unavailable, in miliseconds
    private static final String NEXT_SNAPSHOT_UNAVAILABLE_SLEEP_KEY = "bruce.nextSnapshotUnavailableSleep";
    // This default value may need some tuning. 100ms seemed too small, 1s might be right
    private static int NEXT_SNAPSHOT_UNAVAILABLE_SLEEP_DEFAULT = 1000;

    // How long to wait when 'next' snapshot was available, in miliseconds
    private static final String NEXT_SNAPSHOT_AVAILABLE_SLEEP_KEY = "bruce.nextSnapshotAvailableSleep";
    // This default value may need some tuning. Assuming 1s might be right
    private static int NEXT_SNAPSHOT_AVAILABLE_SLEEP_DEFAULT = 1000;

    private static final String selectLastSnapshotQuery =
	"select * from bruce.slavesnapshotstatus where clusterid = ?";
    // Input for MessageFormat.format()
    private static final String nextSnapshotQuery =
	"select * from bruce.snapshotlog_{0} "+
	" where id > ? "+
	" order by id desc limit 1";
    private static final String updateLastSnapshotQuery =
	"update bruce.slavesnapshotstatus "+
	"   set slave_xaction = ?,  master_id = ?, master_min_xaction = ?, master_max_xaction = ?, "+
	"       master_outstanding_xactions = ?, update_time = now() "+
	" where clusterid = ?";
    private static final String slaveTransactionIdQuery =
	"select * from pg_locks where pid = pg_backend_pid() and locktype = 'transactionid'";
    private static final String tempTableName = "tmpxactions";
    private static final String createTempTable =
	"create temporary table "+tempTableName+" as select * from bruce.transactionlog_{0} limit 0";
    private static final String dropTempTable = "drop table "+tempTableName;
    private static final String insertTempTable = 
	"insert into "+tempTableName+
	"(rowid,xaction,cmdtype,tabname,info) "+
	"values (?,?,?,?,?)";
    // Input for MessageFormat.format()
    private static final String getOutstandingTransactionsQuery =
	"select * from bruce.transactionlog_{0} where xaction >= ? and xaction < ?";
    private static final String determineLatestLogQuery = 
	"select max(id) as id from bruce.currentlog_{0}";
    private static final String getMasterSnapshotLogQuery =
	"select * from bruce.snapshotlog_{0} where id > ? and id <= ?";
    private static final String populateSlaveSnapshotLogQuery =
	"insert into bruce.snapshotlog_{0}_{1} (id,min_xaction,max_xaction,outstanding_xactions) "+
	"values (?,?,?,?)";
    private static final String populateSlaveTransactonLogQuery =
	"insert into bruce.transactionlog_{0}_{1} (rowid,xaction,cmdtype,tabname,info) "+
	"select rowid,xaction,cmdtype,tabname,info from "+tempTableName;
    private static final String deleteUnreplicatedTransactionsQuery =
	"delete from "+tempTableName+" "+
	"where tabname not in "+
	"(select n.nspname||'.'||c.relname as tablename from pg_class c, pg_namespace n "+
	"  where c.relnamespace = n.oid "+
	"   and c.oid in (select tgrelid from pg_trigger "+
	"                  where tgfoid = (select oid from pg_proc "+
	"                                   where proname = 'denyaccesstrigger' "+
	"                                     and pronamespace = (select oid from pg_namespace "+
	"                                                          where nspname = 'bruce'))))";
    private static final String applyTransactionsQuery =
	"select bruce.applyLogTransaction(cmdtype,tabname,info) "+
	"  from "+tempTableName+" order by rowid";
    private static final String daemonModeQuery = "select bruce.daemonmode()";
    private static final String normalModeQuery = "select bruce.normalmode()";
}
