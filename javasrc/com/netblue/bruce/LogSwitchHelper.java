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

import com.netblue.bruce.cluster.Cluster;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LogSwitchHelper {

    private static final Logger logger = Logger.getLogger(LogSwitchHelper.class);
    // Properties that drive actions for this thread
    //
    // How often to create new transaction and snapshot log tables
    private static final String ROTATE_KEY = "bruce.rotateTime";
    private static final int ROTATE_DEFAULT = 1440; // One day
    // How often to retain transaction and snapshot log tables
    private static final String RETAIN_KEY = "bruce.retainTime";
    private static final int RETAIN_DEFAULT = 7200; // Five days

    private static final String THREAD_ITERATION_DELAY_KEY = "bruce.logSwitchDelay";
    private static final int THREAD_ITERATION_DELAY_DEFAULT = 60000;
    // Base name of Table containing row per transaction/schema log table
    // Actual table name is <base>_<cluster_id>
    private static final String CURRENT_LOG_KEY = "bruce.currentLogTableName";
    private static final String CURRENT_LOG_DEFAULT = "bruce.currentlog";
    // Name of transaction log view. Names of tables are <viewname>_<number>
    private static final String TRANSACTION_VIEW_NAME_KEY = "bruce.transactionViewName";
    private static final String TRANSACTION_VIEW_NAME_DEFAULT = "bruce.transactionlog";
    // Name of snapshot log view. Names of tables are <viewname>_<number>
    private static final String SNAPSHOT_VIEW_NAME_KEY = "bruce.snapshotViewName";
    private static final String SNAPSHOT_VIEW_NAME_DEFAULT = "bruce.snapshotlog";

    private final int rotateFrequency;
    private final int retainFrequency;
    private final DataSource ds;
    private final Long clusterId;
    private final String currentLogTableName;
    private final String transactionViewName;
    private final String snapshotViewName;
    private final long threadDelay;
    private long lastSwitch = 0;

    // Entry point for tests at a lower level than where a Cluster exists.
    // Should not be used directly by Daemon or Admin code
    public LogSwitchHelper(BruceProperties p, DataSource ds, Long id) {
	this.clusterId=id;
        this.ds = ds;
        rotateFrequency = p.getIntProperty(ROTATE_KEY, ROTATE_DEFAULT);
        logger.debug("rotateFrequency: " + rotateFrequency);
        retainFrequency = p.getIntProperty(RETAIN_KEY, RETAIN_DEFAULT);
        logger.debug("retainFrequency: " + retainFrequency);
        currentLogTableName = p.getProperty(CURRENT_LOG_KEY, CURRENT_LOG_DEFAULT)+"_"+id.toString();
        transactionViewName = 
	    p.getProperty(TRANSACTION_VIEW_NAME_KEY, TRANSACTION_VIEW_NAME_DEFAULT)+"_"+id.toString();
        snapshotViewName = p.getProperty(SNAPSHOT_VIEW_NAME_KEY, SNAPSHOT_VIEW_NAME_DEFAULT)+"_"+id.toString();
        threadDelay = p.getIntProperty(THREAD_ITERATION_DELAY_KEY, THREAD_ITERATION_DELAY_DEFAULT);
    }

    public LogSwitchHelper(BruceProperties p, DataSource ds, Cluster cl) {
	this(p,ds,cl.getId());
    }

    public void doSwitch() throws SQLException {
	// Is it even time to try and perform a switch
	if ((System.currentTimeMillis() - lastSwitch) > threadDelay) {
	    Connection c = ds.getConnection();
	    try {
		c.setAutoCommit(false);
		Statement s=c.createStatement();
		try {
		    newLogTable(s);
		    dropLogTable(s);
		    c.commit();
		} finally { s.close(); }
	    } finally { c.close(); }
	    lastSwitch=System.currentTimeMillis();
	}
    }

    // If the rotate time has passed for the latest transaction/snapshot logs, create new tables and
    // relevant views
    //
    // public because we use this method in places to guarentee that at least one snapshot/transaction log
    // exists.
    public void newLogTable(Statement s) throws SQLException
    {
        logger.debug("Figuring out if its time to create a new log table.");
        ResultSet r = s.executeQuery("select now()>create_time + interval '" + rotateFrequency + " minutes'" +
                "  from " + currentLogTableName +
                " where id = (select max(id) from " + currentLogTableName + ")");
        // 'not r.next()' covers the case where there are no transaction/snapshot logs, and we need at least one.
        // r.getBoolean(1) covers the case where we have at least one transaction/snapshot log(s), but the
        // most recent one is older than the rotate time and we need to create a new one.
        if ((!r.next()) || r.getBoolean(1))
        {
            logger.info("Time to create a new log table.");
            s.executeUpdate("insert into " + currentLogTableName + "(create_time) values(now())");
            r = s.executeQuery("select max(id) from " + currentLogTableName);
            r.next();
            String newTransactionTableName = transactionViewName + "_" + r.getString(1);
            String newSnapshotTableName = snapshotViewName + "_" + r.getString(1);
            r.close();
            logger.info("New Transaction Table Name is:" + newTransactionTableName);
            logger.info("New Snapshot Table Name is:" + newSnapshotTableName);
            s.executeUpdate("create table " + newTransactionTableName + " (" +
                    " rowid bigint DEFAULT nextval('" + transactionViewName + "_rowseq'::regclass)," +
                    " xaction bigint," +
                    " cmdtype character(1)," +
                    " tabname text," +
                    " info text, " +
                    "PRIMARY KEY (xaction,rowid))");
	    s.executeUpdate("grant all on "+newTransactionTableName+" to public");
	    // newSnapshotTableName already starts with bruce.
            s.executeUpdate("create table " + newSnapshotTableName + " (" +
			    " id bigint primary key default nextval('"+ snapshotViewName + "_idseq'::regclass),"+
			    " min_xaction bigint NOT NULL," +
			    " max_xaction bigint NOT NULL," +
			    " outstanding_xactions text, "+
			    " update_time timestamp without time zone default now())");
	    s.executeUpdate("grant all on "+newSnapshotTableName+" to public");
            dropView(s);
            newView(s);
        }
        else
        {
            logger.debug("Nope, not time yet.");
        }
        r.close();
    }

    // If the retention time has passed for the oldest transaction/snapshot logs, then drop it and recreate the
    // relevant views.
    private void dropLogTable(Statement s) throws SQLException
    {
        logger.debug("Figureing out of its time to drop an old transaction/snapshot log table(s).");
        // The where clause is constructed to keep us from droping the only table......
        ResultSet r = s.executeQuery("select now() > create_time + interval '" + retainFrequency + " minutes', " +
                "       id " +
                "  from " + currentLogTableName +
                " where id = (select min(id) from " + currentLogTableName + ") " +
                "   and id != (select max(id) from " + currentLogTableName + ")");
        if (r.next() && r.getBoolean(1))
        {
            logger.info("Time to drop old tables.");
            int idi = r.getInt(2);
            String oldTransactionTableName = transactionViewName + "_" + idi;
            String oldSnapshotTableName = snapshotViewName + "_" + idi;
            logger.info("Dropping " + oldTransactionTableName + " and " + oldSnapshotTableName);
            dropView(s);
            s.executeUpdate("drop table " + oldTransactionTableName);
            s.executeUpdate("drop table " + oldSnapshotTableName);
            s.executeUpdate("delete from " + currentLogTableName + " where id = " + idi);
            newView(s);
        }
        else
        {
            logger.debug("Nope, not time yet.");
        }
        r.close();
    }

    private void dropView(Statement s) throws SQLException
    {
        dropView(s, transactionViewName);
        dropView(s, snapshotViewName);
    }

    private void dropView(Statement s, String viewName) throws SQLException
    {
        // Does the view even exist?
        ResultSet r = s.executeQuery("select * from pg_views " +
                "where schemaname = split_part('" + viewName + "','.',1) " +
                "and viewname = split_part('" + viewName + "','.',2) ");
        if (r.next())
        {
            s.executeUpdate("drop view " + viewName);
        }
        r.close();
    }

    private void newView(Statement s) throws SQLException
    {
        newView(s, transactionViewName);
        newView(s, snapshotViewName);
    }

    // We can assume that the view does not exist
    private void newView(Statement s, String viewName) throws SQLException
    {
        logger.debug("Recreating view:" + viewName);
        String createViewS = "create view " + viewName + " as ";
        ResultSet r = s.executeQuery("select id from " + currentLogTableName);
        while (r.next())
        {
            createViewS += "select * from " + viewName + "_" + r.getString(1);
            if (!r.isLast())
            {
                createViewS += " union all ";
            }
        }
        s.executeUpdate(createViewS);
	s.executeUpdate("grant all on "+viewName+" to public");
        r.close();
    }
}
