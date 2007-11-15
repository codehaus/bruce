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
package com.netblue.bruce.cluster;

import com.netblue.bruce.SchemaUnitTestsSQL;
import com.netblue.bruce.cluster.Cluster;
import com.netblue.bruce.cluster.ClusterFactory;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.junit.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Tests the {@link com.netblue.bruce.cluster.RegExReplicationStrategy} class
 * @author lanceball
 * @version $Id$
 */
public class RegExReplicationStrategyTest {

    @BeforeClass public static void setupBeforeClass() 
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
    }

    @Before public void setupBefore() throws IllegalAccessException, InstantiationException {
	System.setProperty("hibernate.connection.url",buildUrl("bruce_config"));
	System.setProperty("hibernate.connection.username","bruce");
	ClusterFactory cf = ClusterFactory.getClusterFactory();
	try {
	    Cluster c = cf.getCluster(CLUSTER_NAME);
	    m = c.getMaster();
	    assertEquals("Unexpected master node name","Cluster 0 - master",m.getName());
	    Set<Node> s = c.getSlaves();
	    assertEquals("Unexpected number of slaves",2,s.size());
	    s1 = s.toArray(new Node[0])[0];
	    s2 = s.toArray(new Node[0])[1];
	    if (s1.getName().equals("Cluster 0 - slave 2")) {
		Node t = s2;
		s2 = s1;
		s1 = t;
	    }
	    assertEquals("Unexpected Name for Slave 1","Cluster 0 - slave 1",s1.getName());
	    assertEquals("Unexpected Name for Slave 2","Cluster 0 - slave 2",s2.getName());
	} finally { cf.close(); }
    }

    @Test public void testGetTablesForSchema() throws SQLException {
	for (Node n:new Node[]{m,s1,s2}) {
	    BasicDataSource bds = createDataSource(n.getUri());
	    try {
		RegExReplicationStrategy rS = new RegExReplicationStrategy(bds);
		// Get the tables and validate
		ArrayList<String> tables = rS.getTables(n, REGEXTEST_SCHEMA);
		assertEquals(n.getName()+" Unexpected number of tables returned: "+tables,4,tables.size());
		for (String s:new String[]{"regextest.blue","regextest.green","regextest.orange","regextest.red"}) {
		    assertTrue(n.getName()+" Unexpected table returned: "+s,tables.contains(s));
		}
	    } finally { bds.close(); }
	}
    }

    @Test public void testGetTablesWithoutSchema() throws SQLException {
	for (Node n:new Node[]{m,s1,s2}) {
	    BasicDataSource bds = createDataSource(n.getUri());
	    try {
		RegExReplicationStrategy rS = new RegExReplicationStrategy(bds);
		ArrayList<String> tables = rS.getTables(n,null);
		assertEquals(n.getName()+" Unexpected number of tables returned: "+tables,10,tables.size());
		for (String s:new String[]{
			"public.test1","public.test2","regextest.blue","regextest.green","regextest.orange",
			"regextest.red","regextest_s2.blue2","regextest_s2.green2","regextest_s2.orange2",
			"regextest_s2.red2"}) {
		    assertTrue(n.getName()+" Unexpected table returned: "+s,tables.contains(s));
		}
	    } finally { bds.close(); }
	}
    }
    
    private final static Logger logger = Logger.getLogger(RegExReplicationStrategyTest.class);
    private final static String CLUSTER_NAME = "Cluster Un";
    private final static String REGEXTEST_SCHEMA = "regextest";
    private Node m;                                                                      
    private Node s1;                                                                       
    private Node s2;                                                                       

}
