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

import com.netblue.bruce.cluster.ClusterFactory;
import com.netblue.bruce.cluster.Cluster;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.junit.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

//import java.nio.ByteBuffer;
//import java.util.SortedSet;
import java.io.IOException;
import java.sql.*;
import static java.text.MessageFormat.format;

/**
 * @author rklahn
 * @version $Id$
 */
public class PgExtensionTest {
    @BeforeClass public static void setupBeforeClass() 
	throws SQLException, IllegalAccessException, InstantiationException, IOException,
	       InterruptedException {
	createNamedTestDatabase("bruce");
	// Create a cluster. Master only. No tables in replication
	String[] args = new String[]{"-data",
				     getTestDataDir() + "/master-self-slave-empty.xml",
				     "-initnodeschema",
				     "-loadschema",
				     "-operation", "CLEAN_INSERT",
				     "-url", buildUrl("bruce")};
	com.netblue.bruce.admin.Main.main(args);
	// Setup hibernate, etc.
	System.setProperty("hibernate.connection.url",buildUrl("bruce"));
	System.setProperty("hibernate.connection.username","bruce");
	cf = ClusterFactory.getClusterFactory();
	cl = cf.getCluster(CLUSTER_NAME);
	//
	mDS=createDataSource(cl.getMaster().getUri());
	// We need at least one snapshot/transaction log table for these tests to work. 
	LogSwitchHelper lt = new LogSwitchHelper(new BruceProperties(),mDS,cl);
	Connection c = mDS.getConnection();
	try {
	    Statement s = c.createStatement();
	    try {
		lt.newLogTable(s);
		(new SchemaUnitTestsSQL()).buildDatabase(mDS);
		(new SchemaUnitTestsTriggers()).buildDatabase(mDS);
		// Setup first snapshot, setup slave snapshot
		executeAndLog(s,"select bruce.logsnapshot()");
		executeAndLog(s,
			      "insert into bruce.slavesnapshotstatus "+
			      "(clusterid,slave_xaction,master_id,master_min_xaction,master_max_xaction,"+
			      " master_outstanding_xactions,update_time) "+
			      "select "+cl.getId()+",1,id,min_xaction,max_xaction,outstanding_xactions,"+
			      "       now() "+
			      "  from snapshotlog_"+cl.getId()+
			      " where id = (select max(id) from snapshotlog_"+cl.getId()+")");
	    } finally {
		s.close();
	    }
	} finally {
	    c.close();
	}
    }

    @AfterClass public static void teardownAfterClass() throws SQLException {
	mDS.close();
	cf.close();
    }

    @Test public void denyAccessTrigger() throws SQLException {
	Connection c = mDS.getConnection();
	try {
	    Statement s = c.createStatement();
	    try {
		// We are expecting to throw SQLException, normaly not able to update a replication table on a slave
		try { 
		    executeAndLog(s,"insert into test2(id) values (42)");
		    Assert.fail("Something has gone very, very wrong. We were able to insert into the table.");
		} catch (SQLException e) {}
		// Now that we are going to be in daemon mode, the insert should work
		executeAndLog(s,"select daemonmode()");
		executeAndLog(s,"insert into test2(id) values (42)");
		executeAndLog(s,"delete from test2");
		executeAndLog(s,"select normalmode()");
		// Now that we are back to normal mode, expecting to throw SQLException again
		try { 
		    executeAndLog(s,"insert into test2(id) values (42)");
		    Assert.fail("Something has gone very, very wrong. We were able to insert into the table.");
		} catch (SQLException e) {}
	    } finally {
		s.close();
	    }
	} finally {
	    c.close();
	}
    }

    @Test public void insertTransaction() throws SQLException {
	insertBaseRows();
	applyLoggedTransactions();
	testTestTablesEqual();
    }

    @Test public void updateTransaction() throws SQLException {
	insertBaseRows();
	Connection c = mDS.getConnection();
	try {
	    Statement s = c.createStatement();
	    try {
		executeAndLog(s,"update test1 set c_text = c_int");
		executeAndLog(s,"update test1 set c_bytea = decode(c_text,'escape')");
	    } finally {
		s.close();
	    }
	} finally {
	    c.close();
	}
	applyLoggedTransactions();
	testTestTablesEqual();
    }
    
    @Test public void deleteTransaction() throws SQLException {
	insertBaseRows();
	Connection c = mDS.getConnection();
	try {
	    Statement s = c.createStatement();
	    try {
		executeAndLog(s,"delete from test1");
	    } finally {
		s.close();
	    }
	} finally {
	    c.close();
	}
	applyLoggedTransactions();
	logger.info("DONE replicating");
	testTestTablesEqual();
	logger.info("DELETEs successfuly replicated");
    }

    private void insertBaseRows() throws SQLException {
	int rowsToTest = 10;
	Connection c = mDS.getConnection();
	try {
	    Statement s = c.createStatement();
	    try {
		for (int i=0;i<rowsToTest;i++) {
		    executeAndLog(s,format("insert into test1(c_int) values ({0,number,#})",
					   Math.ceil((Math.random() - 0.5) * 100000)));
		}
	    } finally {
		s.close();
	    }
	} finally {
	    c.close();
	}
    }

    private void applyLoggedTransactions() throws SQLException {
 	Connection c = mDS.getConnection();
 	try {
 	    c.setAutoCommit(false);
 	    c.setSavepoint();
	    Statement s = c.createStatement();
	    try {
 		executeAndLog(s,"select bruce.logsnapshot()");
 		c.commit();
 		ResultSet rs = executeQueryAndLog(s,"select * from slavesnapshotstatus where clusterid = "+cl.getId());
 		rs.next();
 		Snapshot slaveS = new Snapshot(rs.getLong("master_id"),
 					       new TransactionID(rs.getLong("master_min_xaction")),
 					       new TransactionID(rs.getLong("master_max_xaction")),
 					       rs.getString("master_outstanding_xactions"));
		rs = executeQueryAndLog(s,
					"select * from snapshotlog_"+cl.getId()+
					" where id = (select max(id) from snapshotlog_"+cl.getId()+")");
		rs.next();
		Snapshot masterS = new Snapshot(rs.getLong("id"),
						new TransactionID(rs.getLong("min_xaction")),
						new TransactionID(rs.getLong("max_xaction")),
						rs.getString("outstanding_xactions"));
		PreparedStatement ps = c.prepareStatement("select * from bruce.transactionlog_"+cl.getId()+
							  " where xaction >= ? and xaction < ? order by rowid asc");
		ps.setLong(1,slaveS.getMinXid().getLong());
		ps.setLong(2,masterS.getMaxXid().getLong());
		rs = executePreparedQueryAndLog(ps);
		executeAndLog(s,"select bruce.daemonmode()");
		while (rs.next()) {
		    TransactionID tid = new TransactionID(rs.getLong("xaction"));
		    if (slaveS.transactionIDGE(tid) && masterS.transactionIDLT(tid)) {
			executeAndLog(s,
				      "select applyLogTransaction('"+
				      rs.getString("cmdtype")+"','"+
				      "public.test2"+"','"+
				      rs.getString("info")+"')");
		    }
		}
		executeAndLog(s,"select bruce.normalmode()");
		// Update the slave replication status
		ps = c.prepareStatement("update slavesnapshotstatus "+
					"   set slave_xaction = 1, master_id = ?, "+
					"       master_min_xaction = ?, master_max_xaction = ?, "+
					"       master_outstanding_xactions = ?, update_time = now() "+
					" where clusterid = "+cl.getId());
		ps.setLong(1,masterS.getId());
		ps.setLong(2,masterS.getMinXid().getLong());
		ps.setLong(3,masterS.getMaxXid().getLong());
		ps.setString(4,masterS.getInFlight());
		executePreparedAndLog(ps);
		c.commit();
	    } finally {
		s.close();
	    }
 	} finally {
 	    c.close();
 	}
    }

    private void testTestTablesEqual() throws SQLException {
	Connection c = mDS.getConnection();
	try {
	    Statement s1 = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
	    try {
		Statement s2 = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
		try {
		    ResultSet rs1 = executeQueryAndLog(s1,"select id,c_bytea,c_text,c_int from test1 order by id");
		    ResultSet rs2 = executeQueryAndLog(s2,"select id,c_bytea,c_text,c_int from test2 order by id");
		    // Obtain the row counts, which must be equal
		    rs1.last();
		    rs2.last();
		    int test1RowCount = rs1.getRow();
		    int test2RowCount = rs2.getRow();
		    assertEquals("row counts of test1 ("+test1RowCount+") "+
				 "and test2 ("+test2RowCount+") are not the same",
				 test1RowCount,test2RowCount);
		    rs1.beforeFirst();
		    rs2.beforeFirst();
		    while (rs1.next() && rs2.next()) {
			for (String field : new String[]{"id","c_bytea","c_text","c_int"}) {
			    assertEquals(field+" not the same between tables",
					 rs1.getString(field),
					 rs2.getString(field));
			}
		    }
		} finally {
		    s2.close();
		}
	    } finally {
		s1.close();
	    }
	} finally {
	    c.close();
	}
    }

    private final static Logger logger = Logger.getLogger(PgExtensionTest.class);
    private final static String CLUSTER_NAME = "Cluster Un";
    private static BasicDataSource mDS;
    private static Cluster cl;
    private static ClusterFactory cf;
}
