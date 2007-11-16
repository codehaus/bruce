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
package com.netblue.bruce.cluster.persistence;

import com.netblue.bruce.cluster.Cluster;
import com.netblue.bruce.cluster.ClusterFactory;
import com.netblue.bruce.cluster.ClusterInitializationException;
import com.netblue.bruce.cluster.Node;
import org.apache.log4j.Logger;
import org.junit.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import java.util.Set;

/**
 * Tests {@link com.netblue.bruce.cluster.persistence.PersistentClusterFactory}
 * @author lanceball
 */
public class PersistentClusterFactoryTest {

    @BeforeClass public static void setupBeforeClass() 
	throws SQLException, IllegalAccessException, InstantiationException, InterruptedException {
	createNamedTestDatabase("bruce");
	// Create the cluster. Master with two slaves. Several tables in replication.               
        com.netblue.bruce.admin.Main.main(new String[]{
                "-data",getTestDataDir()+"/master-only-empty.xml",
                "-initnodeschema",
                "-initsnapshots","MASTER",
                "-loadschema",
                "-operation","CLEAN_INSERT",
                "-url",buildUrl("bruce")
            });
	System.setProperty("hibernate.connection.url",buildUrl("bruce"));
        System.setProperty("hibernate.connection.username","bruce");
	defaultFactory = ClusterFactory.getClusterFactory();
	assertNotNull("Can not instantiate defaultFactory",defaultFactory);
    }

    @AfterClass public static void teardownAfterClass() {
	defaultFactory.close();
    }

    @Test public void testDefaultCluster() {
	// Test that we get what we expect whit the default cluster
	Cluster cluster = defaultFactory.getCluster(ClusterFactory.DEFAULT_CLUSTER_NAME);
	assertNotNull("Cluster should not be null", cluster);
	assertEquals("Unexpected default Cluster instance", ClusterFactory.DEFAULT_CLUSTER_NAME, cluster.getName());
    }

    /**
     * Tests whether the ClusterFactory creates more than one cluster for a given name
     */
    @Test public void testSingletonClusterInstances() {
	// first test a cluster that exists.
	Cluster c = defaultFactory.getCluster(existsClusterName);
	assertSame("PersistentClusterFactory should only create one instance for a given cluster", 
		   c, defaultFactory.getCluster(existsClusterName));
	
	// And one that does not exist
	c = defaultFactory.getCluster(doesNotExistClusterName);
	assertSame("PersistentClusterFactory should only create one instance for a given cluster", 
		   c, defaultFactory.getCluster(doesNotExistClusterName));
    }
    
    @Test(expected=ClusterInitializationException.class) public void testNullName() {
	Cluster cluster = defaultFactory.getCluster(null);
	fail("Cluster initialization should fail with a null name");
    }

    @Test public void testClusterInitialization() {
	Cluster c = defaultFactory.getCluster(existsClusterName);
	assertNotNull("Test cluster should not be null", c);

	// now check that it has the data we expect from the database
	assertEquals("Wrong ID Found", 1000L, c.getId());
	assertEquals("Wrong name found", existsClusterName, c.getName());

	// Now check for the master node
	Node master = c.getMaster();
	assertNotNull("Master node for cluster should not be null", master);
	assertEquals("Unexpected name for master found", "Cluster 0 - Primary master", master.getName());
	assertEquals("Unexpected ID for master found", 1L, master.getId());
	assertTrue("Master should be available", master.isAvailable());

	// Now check for the slave nodes (there arent any)
	Set<Node> slaves = c.getSlaves();
	assertNotNull("Slave node set should not be null", slaves);
	assertEquals("Unexpected length for slave node set", 0, slaves.size());
    }

    private static final Logger logger = Logger.getLogger(PersistentClusterFactoryTest.class);
    private static final String existsClusterName = "Cluster Un";
    private static final String doesNotExistClusterName = "foobar";
    private static ClusterFactory defaultFactory;
}
