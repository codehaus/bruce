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
import com.netblue.bruce.cluster.ClusterInitializationException;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.junit.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.*;

/**
 * Tests the ReplicationDaemon class
 *
 * @author lanceball
 * @version $Id$
 */
public class ReplicationDaemonTest {

    @BeforeClass public static void setupBeforeClass() 
	throws SQLException, IOException, InterruptedException {
	for (String dbS : new String[]{"bruce_master","bruce_slave_1","bruce_slave_2"}) {
	    createNamedTestDatabase(dbS);
	    BasicDataSource bds = createDataSource(buildUrl(dbS));
	    try {
		(new SchemaUnitTestsSQL()).buildDatabase(bds);
	    } finally {
		bds.close();
	    }
	}
	// Create a cluster. Master with two slaves. Several tables in replication.
	com.netblue.bruce.admin.Main.main(new String[]{
		"-data",getTestDataDir()+"/replicate-unit-tests.xml",
		"-initnodeschema",
		"-initsnapshots","MASTER",
		"-loadschema",
		"-operation","CLEAN_INSERT",
		"-url",buildUrl("bruce_master")
	    });
	// Setup hibernate
	System.setProperty("hibernate.connection.url",buildUrl("bruce_master"));
	System.setProperty("hibernate.connection.username","bruce");
    }

    @Test public void testLoadClusterNullName() {
	try {
            ReplicationDaemon daemon = new ReplicationDaemon();
            daemon.loadCluster(null);
            fail("ReplicationDaemon should throw an exception if a null cluster name is provided");
	} catch (ClusterInitializationException e) {}
    }

    @Test public void testRunNoClusterInitialized() {
        try {
            ReplicationDaemon daemon = new ReplicationDaemon();
            daemon.run();
            fail("ReplicationDaemon should throw an exception if it's run without loading a cluster");
        } catch (ClusterInitializationException e) {}
    }

    @Test public void testLoadCluster() {
        ReplicationDaemon daemon = new ReplicationDaemon();
        daemon.loadCluster(CLUSTER_NAME);
        Cluster cluster = daemon.getCluster();
        assertNotNull("Cluster was not loaded", cluster);
        assertEquals(CLUSTER_NAME, cluster.getName());
        assertNotNull(cluster.getMaster());
        assertEquals(2, cluster.getSlaves().size());
        daemon.shutdown();
    }

    @Test public void testRun() {
        System.setProperty("bruce.logSwitchDelay", "10000");
        ReplicationDaemon daemon = new ReplicationDaemon();
        daemon.loadCluster(CLUSTER_NAME);
        daemon.run();
        assertEquals(2, daemon.getActiveSlaveCount());
        daemon.shutdown();
    }

    private static final Logger logger = Logger.getLogger(ReplicationDaemonTest.class);
    public static final String CLUSTER_NAME = "Cluster Un";
}
