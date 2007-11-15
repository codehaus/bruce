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
import com.netblue.bruce.cluster.ClusterFactory;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.junit.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.*;

/**
 * Unit tests for the SlaveFactory class
 * @author lanceball
 * @version $Id$
 */
public class SlaveFactoryTest {

    @BeforeClass public static void setupBeforeClass() 
	throws SQLException, IOException, IllegalAccessException, InterruptedException, InstantiationException {
	// Create all databases
	for (String dbS : new String[]{"bruce_config","bruce_master","bruce_slave_1","bruce_slave_2"}) {
	    createNamedTestDatabase(dbS);
	}
	// Add test schema to all dbs minus the config db
	for (String dbS : new String[]{"bruce_master","bruce_slave_1","bruce_slave_2"}) {
	    BasicDataSource bds = createDataSource(buildUrl(dbS));
	    try {
		(new SchemaUnitTestsSQL()).buildDatabase(bds);
	    } finally { bds.close(); }
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

    @AfterClass public static void teardownAfterClass() {
	cf.close();
    }

    @Test public void testNewThread() {
        SlaveFactory factory = new SlaveFactory(cl);
        assertNotNull(factory.newThread(new SimpleRunnable()));
        factory.shutdown();
    }

    @Test (expected=IllegalArgumentException.class) public void testNullConstructor() {
	new SlaveFactory(null);
	fail("SlaveFactory should fail with null constructor arguments");
    }

    private static final class SimpleRunnable implements Runnable {
        public void run() {
            try { Thread.sleep(1); }
            catch (InterruptedException e) {}
        }
    }

    @Test public void testSpawnSlaves() {
	SlaveFactory slaveFactory = new SlaveFactory(cl);
	ThreadGroup threadGroup = slaveFactory.spawnSlaves();
	assertNotNull("ThreadGroup from SlaveFactory should not be null", threadGroup);

	// The thread group should be named the same as the cluster
	assertEquals(threadGroup.getName(), cl.getName());
	// should be a daemon thread group (destroyed when last thread is stopped
	assertTrue("ThreadGroup for cluster should be a daemon", threadGroup.isDaemon());
	// should be valid
        assertFalse(threadGroup.isDestroyed());

	// TODO:  This test is fragile and will probably break as we continue implementation
	// Should contain the same number of threads as our cluster
	final int slaveCount = cl.getSlaves().size();
	Thread[] threads = new Thread[slaveCount];
	threadGroup.enumerate(threads);
	assertEquals(slaveCount, threadGroup.activeCount());
	assertNotNull(threads[0]);
	assertNotNull(threads[1]);
	slaveFactory.shutdown();
    }

    private final static Logger logger = Logger.getLogger(SetupClusterFromExistingDbAcceptanceTest.class);
    private final static String CLUSTER_NAME = "Cluster Un";
    private static ClusterFactory cf;
    private static Cluster cl;
}
