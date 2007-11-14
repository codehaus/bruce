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
import org.apache.commons.dbcp.cpdsadapter.DriverAdapterCPDS;
import org.apache.commons.dbcp.datasources.SharedPoolDataSource;
import org.apache.log4j.Logger;
import org.junit.*;
import static org.junit.Assert.*;
import static com.netblue.bruce.TestDatabaseHelper.executeAndLog;

import java.sql.*;
import java.text.MessageFormat;

/**
 * @author rklahn
 * @version $Id$
 */
public class MortalUserCanModifyTableTest {
    @BeforeClass public static void setupTestClass() 
	throws SQLException, IllegalAccessException, InstantiationException, 
	       ClassNotFoundException, InterruptedException {
	TestDatabaseHelper.createNamedTestDatabase("bruce");
	// Create a cluster. Master only. No tables in replication
	String[] args = new String[]{"-data",
				     TestDatabaseHelper.getTestDataDir() + "/master-only-empty.xml",
				     "-initnodeschema",
				     "-loadschema",
				     "-operation", "CLEAN_INSERT",
				     "-url", TestDatabaseHelper.buildUrl("bruce")};
	com.netblue.bruce.admin.Main.main(args);
	ClusterFactory clusterFactory = ClusterFactory.getClusterFactory();
	Cluster cluster = clusterFactory.getCluster(CLUSTER_NAME);
	DriverAdapterCPDS cpds = new DriverAdapterCPDS();
	cpds.setDriver(System.getProperty("bruce.jdbcDriverName","org.postgresql.Driver"));
	cpds.setUrl(TestDatabaseHelper.baseUrlFromUrl(cluster.getMaster().getUri()));
	cpds.setUser("bruce");
	cpds.setPassword("bruce");
	masterDataSource = new SharedPoolDataSource();
	masterDataSource.setConnectionPoolDataSource(cpds);
	Connection c = masterDataSource.getConnection();
	try { // Make sure connection gets closed 
	    Statement s = c.createStatement();
	    try { // Make sure statement gets closed
		try { 
		    executeAndLog(s,"drop schema mortal cascade"); 
		} catch (SQLException e) {} // Error OK. Probably schema does not exist.
		try {
		    executeAndLog(s,"drop user mortal");
		} catch (SQLException e) {} // Error OK. Probably use3r does not exist.
		executeAndLog(s,"create user mortal");
		executeAndLog(s,"alter user mortal with LOGIN PASSWORD 'mortal'");
		executeAndLog(s,"create schema AUTHORIZATION mortal");
	    } finally {
		s.close();
	    }
	} finally {
	    c.close();
	}
    }

    @AfterClass public static void teardownAfterClass() throws Exception {
	if (masterDataSource != null) {
	    masterDataSource.close();
	}
    }

    @Test public void testMortalModify() throws SQLException {
	Connection c = masterDataSource.getConnection("mortal","mortal");
	try {
	    Statement s = c.createStatement();
	    try {
		executeAndLog(s,"create table test (id bigserial,t text)");
		executeAndLog(s,MessageFormat.format(CREATE_TX_TRIGGER_STMT,"test","mortal.test"));
		executeAndLog(s,MessageFormat.format(CREATE_SNAP_TRIGGER_STMT,"test","mortal.test"));
		executeAndLog(s,"insert into test (t) values ('greetings')");
		executeAndLog(s,"update test set t = 'salutations'");
		executeAndLog(s,"delete from test");
	    } finally {
		s.close();
	    }
	} finally {
	    c.close();
	}
    }

    private static final Logger logger = Logger.getLogger(MortalUserCanModifyTableTest.class);
    private static final String CLUSTER_NAME = "Cluster Un";
    private static SharedPoolDataSource masterDataSource;
    private static final String CREATE_TX_TRIGGER_STMT = 
	"CREATE TRIGGER {0}_tx AFTER INSERT OR DELETE OR UPDATE ON {1} "+
	"           FOR EACH ROW EXECUTE PROCEDURE bruce.logtransactiontrigger()";
    private static final String CREATE_SNAP_TRIGGER_STMT = 
	"CREATE TRIGGER {0}_sn BEFORE INSERT OR DELETE OR UPDATE ON {1} "+
	"           FOR EACH STATEMENT EXECUTE PROCEDURE bruce.logsnapshottrigger()";
}