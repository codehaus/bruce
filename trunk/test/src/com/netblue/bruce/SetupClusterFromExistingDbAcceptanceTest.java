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

// import org.apache.commons.dbcp.BasicDataSource;
// import org.apache.log4j.Logger;
// import org.junit.AfterClass;
// import org.junit.Assert;
// import org.junit.BeforeClass;
// import org.junit.Test;

// import javax.sql.DataSource;
// import java.sql.Connection;
// import java.sql.ResultSet;
// import java.sql.SQLException;
// import java.sql.Statement;
// import java.util.Properties;

import com.netblue.bruce.admin.Version;
import com.netblue.bruce.cluster.*;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.junit.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.*;

/**
 * Tests http://bi02.yfdirect.net:8080/xplanner/do/view/task?oid=1137 and http://bi02.yfdirect.net:8080/xplanner/do/view/task?oid=1294
 *
 * @author lanceball
 * @version $Id$
 */
public class SetupClusterFromExistingDbAcceptanceTest {

    // Before (vice BeforeClass) because we need the cluster to be in a known state at the
    // begining of the test, and some tests muck with the cluster configuration.
    @Before public void setupBefore() 
	throws SQLException, IOException, IllegalAccessException, InstantiationException, InterruptedException {
	// Create all databases
	for (String dbS : new String[]{"bruce_config","bruce_master","bruce_slave_1","bruce_slave_2"}) {
	    createNamedTestDatabase(dbS);
	}
	// Add test schema to all dbs minus the config db
	for (String dbS : new String[]{"bruce_master","bruce_slave_1","bruce_slave_2"}) {
	    BasicDataSource bds = createDataSource(buildUrl(dbS));
	    try {
		(new SchemaUnitTestsSQL()).buildDatabase(bds);
	    } finally {
		bds.close();
	    }
	}
	// Create the cluster. Master with two slaves. Several tables in replication.
	com.netblue.bruce.admin.Main.main(new String[]{
		"-data",getTestDataDir()+"/replicate-unit-tests.xml",
		"-initnodeschema",
		"-initsnapshots","MASTER",
		"-loadschema",
		"-operation","CLEAN_INSERT",
		"-url",buildUrl("bruce_config")
	    });
	// Setup hibernate
	System.setProperty("hibernate.connection.url",buildUrl("bruce_config"));
	System.setProperty("hibernate.connection.username","bruce");
	cf = ClusterFactory.getClusterFactory();
	cl = cf.getCluster(CLUSTER_NAME);
    }

    @After public void teardownAfter() throws SQLException {
	cf.close();
    }

    @Test public void testSetupCluster() throws SQLException {
	// We expect the correct version of the schema has been installed on all nodes of the cluster
 	BasicDataSource bds = createDataSource(cl.getMaster().getUri());
 	assertTrue("masterDB schema not same version as code",Version.isSameVersion(bds));
 	bds.close();
 	for (Node n:cl.getSlaves()) {
	    bds = createDataSource(n.getUri());
	    assertTrue("slaveDB schema not same version as code",Version.isSameVersion(bds));
	    bds.close();
	}
	// We are expecting the cluster we set up to be the cluster we set up.
	assertClusterConfiguration();
	// We expect the master database to have one row in the snapshot log at this point
	assertOneMSnapshot();
	// We also expect the slave databases to have one row in the slavesnapshotstatus table
	for (Node n:cl.getSlaves()) {
	    assertOneSSnapshot(n);
	}
    }

    @Test public void testDeleteNode() throws SQLException {
	// Delete one node from the cluster
	com.netblue.bruce.admin.Main.main(new String[]{
		"-data", getTestDataDir() + "/admin-test-delete.xml",
		"-operation", "DELETE",
		"-url", buildUrl("bruce_config")		
	    });
	BasicDataSource sDS = createDataSource(buildUrl("bruce_config"));
	try {
	    Connection c = sDS.getConnection();
	    try {
		Statement s = c.createStatement();
		try {
		    ResultSet rs = executeQueryAndLog(s,"select * from node_cluster where node_id = 3");
		    assertFalse("Slave 2 was not deleted from cluster mapping table",rs.next());
		} finally {
		    s.close();
		}
	    } finally {
		c.close();
	    }
	} finally {
	    sDS.close();
	}
    }

    @Test public void testUpdateNode() throws SQLException {
	// Modify a node
	com.netblue.bruce.admin.Main.main(new String[]{
		"-data", getTestDataDir() + "/admin-test-update.xml",
		"-operation", "UPDATE",
		"-url", buildUrl("bruce_config")
	    });
	BasicDataSource sDS = createDataSource(buildUrl("bruce_config"));
	try {
	    Connection c = sDS.getConnection();
	    try {
		Statement s = c.createStatement();
		try {
		    ResultSet rs = executeQueryAndLog(s,"select * from yf_node where id = 2");
		    assertTrue("Unexpected error modifying Slave 1",rs.next());
		    assertEquals("Unexpected value of include table regex","testme",rs.getString("includetable"));
		} finally {
		    s.close();
		}
	    } finally {
		c.close();
	    }
	} finally {
	    sDS.close();
	}
    }

    @Test public void testInsertNode() throws SQLException, InterruptedException {
	createNamedTestDatabase("bruce_slave_3");
	com.netblue.bruce.admin.Main.main(new String[]{
		"-data", getTestDataDir()+"/admin-test-insert.xml",
		"-initnodeschema",
		"-initsnapshots", "MASTER",
		"-operation", "INSERT",
		"-url", buildUrl("bruce_config")
	    });
	BasicDataSource sDS = createDataSource(buildUrl("bruce_config"));
	try {
	    Connection c = sDS.getConnection();
	    try {
		Statement s = c.createStatement();
		try {
		    ResultSet rs = executeQueryAndLog(s,"select * from yf_node where id = 4");
		    assertTrue("Unexpected error inserting Slave 3",rs.next());
		    assertEquals("Unexpected value of include table regex","testme",rs.getString("includetable"));
		} finally {
		    s.close();
		}
	    } finally {
		c.close();
	    }
	} finally {
	    sDS.close();
	}
    }

    private void assertOneSSnapshot(Node sN) throws SQLException {
	String sURI = sN.getUri();
	BasicDataSource sDS = createDataSource(sURI);
	try {
	    Connection c = sDS.getConnection();
	    try {
		Statement s = c.createStatement();
		ResultSet rs = executeQueryAndLog(s,"select count(*) from slavesnapshotstatus");
		assertTrue("Unexpected results from slavesnapshotstatus",rs.next());
		assertEquals("Unexpected number of rows from slavesnapshotstatus",1,rs.getInt(1));
		try {
		} finally {
		    s.close();
		}	
	    } finally {
		c.close();
	    }
	} finally {
	    sDS.close();
	}
    }

    private void assertOneMSnapshot() throws SQLException {
	Long clusterId = cl.getId();
	String mURI = cl.getMaster().getUri();
	BasicDataSource mDS = createDataSource(mURI);
	try {
	    Connection c = mDS.getConnection();
	    try {
		Statement s = c.createStatement();
		try {
		    ResultSet rs = executeQueryAndLog(s,"select count(*) from snapshotlog_"+clusterId.toString());
		    assertTrue("Unexpected results from master snapshot log",rs.next());
		    assertEquals("Unexpected number of rows in master snapshot log",1,rs.getInt(1));
		} finally {
		    s.close();
		}
	    } finally {
		c.close();
	    }
	} finally {
	    mDS.close();
	}
    }

    private void assertClusterConfiguration() throws SQLException {
	// Datasource needed to the admin database
	BasicDataSource adminDS = createDataSource(buildUrl("bruce_config"));
	try {
	    Connection c = adminDS.getConnection();
	    try {
		Statement s = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
		try {
		    // Check that a single cluster exists
		    ResultSet rs = executeQueryAndLog(s,"select * from yf_cluster");
		    // Obtain the row count, should be 'one'
		    rs.last();
		    int rowCount = rs.getRow();
		    assertEquals("Unexpected cluster count",1,rs.getRow());
		    rs.first();
		    assertEquals("Unexpected cluster ID",new Long(1000L),rs.getLong("id"));
		    Long masterId = rs.getLong("master_node_id");
		    // Master check
		    rs =  executeQueryAndLog(s,"select * from yf_node where id = "+masterId.toString());
		    rs.last();
		    assertEquals("Unexpected master count",1,rs.getRow());
		    rs.first();
		    assertEquals("Unexpected master ID",new Long(1L),rs.getLong("id"));
		    assertEquals("Unexpected master includetable regex",
				 "^(public\\..*|regextest\\..*|regextest_s2\\..*)$",
				 rs.getString("includetable"));
		    assertEquals("Unexpected master URI",
				 "jdbc:postgresql://localhost:5432/bruce_master?user=bruce&password=bruce",
				 rs.getString("uri"));
		    // Slave checks
		    //
		    // ClusterID and master node id have already been asserted.
		    rs = executeQueryAndLog(s,"select * from node_cluster where cluster_id = 1000 and node_id != 1");
		    rs.last();
		    assertEquals("Unexpected slave count",2,rs.getRow());
		    for (String nID: new String[]{"3","2"}) {
			rs = executeQueryAndLog(s,
						"select * from yf_node where id = (select node_id from node_cluster "+
						"                                   where cluster_id = 1000 "+
						"                                   and node_id = "+nID+")");
			assertTrue("No slave "+nID+" found",rs.next());
			assertEquals("Unexpected slave includetable regex",
				     "^(public\\..*|regextest\\..*|regextest_s2\\..*)$",
				     rs.getString("includetable"));
			if (nID.equals("3")) {
			    assertEquals("Unexpected slave URI for node 3",
					 "jdbc:postgresql://localhost:5432/bruce_slave_2?user=bruce&password=bruce",
					 rs.getString("uri"));
			}
			if (nID.equals("2")) {
			    assertEquals("Unexpected slave URI for node 2",
					 "jdbc:postgresql://localhost:5432/bruce_slave_1?user=bruce&password=bruce",
					 rs.getString("uri"));
			}
		    }
		} finally {
		    s.close();
		}
	    } finally {
		c.close();
	    }
	} finally {
	    adminDS.close();
	}
    }

    private static final Logger logger = Logger.getLogger(SetupClusterFromExistingDbAcceptanceTest.class);
    private final static String CLUSTER_NAME = "Cluster Un";
    private static Cluster cl;
    private static ClusterFactory cf;



//     private static final Logger LOGGER = Logger.getLogger(SetupClusterFromExistingDbAcceptanceTest.class);
//     private static final String MASTER_DB = "bruce_master";
//     private static final String SLAVE1_DB = "bruce_slave_1";
//     private static final String SLAVE2_DB = "bruce_slave_2";
//     private static final String CONFIG_URL_KEY = "config.url";
//     private static final String MASTER_URL_KEY = "master.url";
//     private static final String SLAVE1_URL_KEY = "slave1.url";
//     private static final String SLAVE2_URL_KEY = "slave2.url";

//     private static Properties properties;

//     private static BasicDataSource masterDS;
//     private static BasicDataSource slave1DS;
//     private static BasicDataSource slave2DS;
//     private static BasicDataSource configDS;
}
