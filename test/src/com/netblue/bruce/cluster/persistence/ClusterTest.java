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

// import org.hibernate.SessionFactory;
// import org.hibernate.cfg.AnnotationConfiguration;
// import org.junit.*;
// import static com.netblue.bruce.TestDatabaseHelper.*;
// import static org.junit.Assert.*;

// import java.sql.SQLException;
// import java.util.Set;

import com.netblue.bruce.cluster.ClusterChangeListener;
import com.netblue.bruce.cluster.Cluster;
import com.netblue.bruce.cluster.ClusterFactory;
import com.netblue.bruce.cluster.Node;
import org.apache.log4j.Logger;
import org.junit.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * @author lanceball
 * @version $Id$
 */
public class ClusterTest {

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
	cf = ClusterFactory.getClusterFactory();
    }

    @AfterClass public static void teardownAfterClass() {
	cf.close();
    }

    @Test public void testGetId() {
        Cluster cluster = cf.getCluster(CLUSTER_NAME);
        assertEquals(1000L, cluster.getId());
    }

    @Test public void testSetGetName() {
        Cluster cluster = cf.getCluster(CLUSTER_NAME);
        final String newName = "This is a new name for the cluster";
        cluster.setName(newName);
        assertEquals(newName, cluster.getName());
    }

    @Test public void testAddSlave() {
	Cluster cl = cf.getCluster(CLUSTER_NAME);

	// A new slave
	Node s = new com.netblue.bruce.cluster.persistence.Node();
	s.setName("A new test slave");
	s.setUri("This is a URI!");

	// A listener to test to see if we get updated when we add the slave
	MockClusterChangeListener ccl = new MockClusterChangeListener();
	cl.addClusterChangeListener(ccl);
	assertFalse("Listener should start with slaveAdded = false",ccl.slaveAdded);
	
	// And add the slave
	cl.addSlave(s);
	assertTrue("Cluster not notified when slave added",ccl.slaveAdded);

	// Get another instance of the same cluster, make sure the slave we just added is there
	Cluster acl = cf.getCluster(CLUSTER_NAME);
	Set<Node> slaves = acl.getSlaves();
	assertEquals("Cluster slave collection not updated correctly",1,slaves.size());

	// Make sure the one we added is in the set
	boolean foundIt = false;
	for (Node n : slaves) {
	    if (n == s) { foundIt = true; }
	}

	assertTrue("Could not find the slave we added in the cluster",foundIt);

	// Make sure the cluster added itself to the slave
	Set<Cluster> pc = s.getCluster();
	assertEquals("Unexpected cluster size",1,pc.size());
	
	// Make sure the number of slaves has not grown
	assertEquals(cl.getSlaves().size(),1);
    }

    @Test public void testRemoveSlave() {
	Cluster cl = cf.getCluster(CLUSTER_NAME);
	// Make sure the cluster has at least one slave
	Node s = new com.netblue.bruce.cluster.persistence.Node();
	s.setName("A new test slave");
	s.setUri("This is a URI!");
	cl.addSlave(s);
	
	// A listener to make sure we are notified of cluster changes
	MockClusterChangeListener ccl = new MockClusterChangeListener();
	cl.addClusterChangeListener(ccl);
	
	// Remove a slave. Might or might not be the one we added, above
	s = cl.getSlaves().iterator().next();
	assertNotNull("Cluster contains no slaves",s);
	assertSame(cl.removeSlave(s),s);
	assertTrue("No notification when slave removed",ccl.slaveRemoved);

	// Try and remove a non existent slave
	ccl.slaveRemoved = false;
	Node os = new com.netblue.bruce.cluster.persistence.Node();
	assertNull("Cluster should return null when a non-existant slave is removed",cl.removeSlave(os));
	assertFalse("Notification when non-existant slave remomved",ccl.slaveRemoved);
    }

    private final static class MockClusterChangeListener implements ClusterChangeListener {
        int slaveAddedCounter = 0;
        int slaveRemovedCounter = 0;
        boolean slaveAdded = false;
        boolean slaveRemoved = false;
        boolean slaveEnabled = false;
        boolean slaveDisabled = false;
        boolean masterReplaced = false;
        boolean masterUnavailable = false;

        public void slaveAdded(Node newSlave) { this.slaveAdded = true; slaveAddedCounter++; }
        public void slaveRemoved(Node oldSlave) { this.slaveRemoved = true; slaveRemovedCounter++; }
        public void slaveDisabled(Node slave) { this.slaveDisabled = true; }
        public void slaveEnabled(Node slave) { this.slaveEnabled = true; }
        public void masterReplaced(Node oldMaster, Node newMaster) { this.masterReplaced = true; }
        public void masterUnavailable(Node master) { this.masterUnavailable = true; }
    }

    private final static Logger logger = Logger.getLogger(ClusterTest.class);
    private final static String CLUSTER_NAME = "Cluster Un";
    private static ClusterFactory cf;
}
