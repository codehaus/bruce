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
package com.netblue.bruce.admin;

import org.apache.commons.dbcp.BasicDataSource;
import com.netblue.bruce.Log4jCapture;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import java.util.Properties;

/**
 * Tests that the admin tool/configuration database can accept an 'includetable' of varing 
 * character lengths, but importantly testing for more than 255 characters. This is the 
 * subject of a filed bug, http://jira.codehaus.org/browse/BRUCE-10
 *
 * @author rklahn
 * @version $Id$
 */

public class IncludetableRegexTest {
    
    @BeforeClass public static void setupBeforeClass() throws SQLException, InterruptedException {
	// Create all databases
	createNamedTestDatabase("bruce_config");
	createNamedTestDatabase("bruce_master");
	// Datasource for config database
	aDS=createDataSource(buildUrl("bruce_config"));
    }
    
    @AfterClass public static void teardownAfterClass() throws SQLException {
	aDS.close();
    }

    @Test public void testS() throws SQLException, IOException {
 	for (String entry : new String[]{"0","1","1269","258","259","27"}) {
	    // Capture current log4j configuration, each test will reconfigure log4j
	    Properties log4jP = Log4jCapture.getLog4jConfig();
 	    com.netblue.bruce.admin.Main.main(new String[]{
		    "-data", getTestDataDir()+"/includetable-regexp-"+entry+".xml",
		    "-loadschema",
		    "-operation", "CLEAN_INSERT",
		    "-url", buildUrl("bruce_config")
		});
	    // Restore the original log4j configuration
	    Log4jCapture.setLog4jConfig(log4jP);
	    Connection c = aDS.getConnection();
	    try {
		Statement s = c.createStatement();
		try {
		    ResultSet rs = executeQueryAndLog(s,"select length(includetable) from bruce.yf_node where id = 1");
		    assertTrue("Unexpected result from includetable length check",rs.next());
		    assertEquals("Unexpected includetable length.",entry,rs.getString(1));
		} finally { s.close(); }
	    } finally { c.close(); }
	}
    }

    private static final Logger logger = Logger.getLogger(IncludetableRegexTest.class);
    private static BasicDataSource aDS;

//     @BeforeClass public static void setupTestClass() throws ClassNotFoundException, SQLException {
// 	// Properties config
// 	properties = new BruceProperties();
// 	// DB connection config
// 	Class.forName("org.postgresql.Driver");
// 	connection = 
// 	    DriverManager.getConnection((String) properties.getProperty("postgresql.adminURL"));
//     }

//     @AfterClass public static void teardownTestClass() {
// 	try {
// 	    connection.close();
// 	    // Error OK. Probably the database went away.
// 	} catch (SQLException e) { }
//     }

//     @Before public void setupTest() throws IOException, SQLException {
// 	// Make bruce database go away.
// 	try {
// 	    connection.createStatement().execute("drop database "+
// 						 properties.getProperty("postgresql.db_name"));
// 	    // Error OK. Probably ERROR: database "bruce" does not exist 
// 	} catch (SQLException e) { }
// 	// And recreate it. This time, errors count.
// 	try {
// 	    connection.createStatement().execute("create database "+
// 						 properties.getProperty("postgresql.db_name"));
// 	} catch (SQLException e) {
// 	    fail("unexpected exception when creating database "+
// 		 "(are other users still connected to the "+
// 		 properties.getProperty("postgresql.db_name")+" database?)");
// 	}
// 	// Capture current log4j configuration, each test will reconfigure log4j
// 	log4jP = Log4jCapture.getLog4jConfig();
// 	// Capture current System.out and System.err, each test will redirect it, and it
// 	// needs restoring @After
// 	originalSysOut = System.out;
// 	originalSysErr = System.err;
// 	// Replace System.out and System.err with our own, in core, versions
//         outBytes = new ByteArrayOutputStream();
//         errBytes = new ByteArrayOutputStream();
//         PrintStream spawnOut = new PrintStream(outBytes);
//         PrintStream spawnErr = new PrintStream(errBytes);
// 	System.setOut(spawnOut);
// 	System.setErr(spawnErr);
//     }

//     @After public void teardownTest() {
// 	// Restore System.out and System.err to their values before test
// 	System.setOut(originalSysOut);
// 	System.setErr(originalSysErr);
// 	// Restore log4j config
// 	Log4jCapture.setLog4jConfig(log4jP);
// 	// Examine test output
// 	String stdout = outBytes.toString();
// 	String stderr = errBytes.toString();
// 	if (stderr.length()>0) {
// 	    fail("admin tool contains unexpected stderr output:\n"+stderr);
// 	}
// 	assertFalse("admin tool failed (bruce_master.bruce.yf_node.includetable should be text and is varchar(255)?):\n"+stdout,stdout.contains("FATAL"));
//     }

    
//     private static final Logger logger = Logger.getLogger(IncludetableRegexTest.class);
//     private static Connection connection;
//     private static BruceProperties properties;
//     private static Properties log4jP;
//     private static PrintStream originalSysOut;
//     private static PrintStream originalSysErr;
//     private static ByteArrayOutputStream outBytes;
//     private static ByteArrayOutputStream errBytes;
}
