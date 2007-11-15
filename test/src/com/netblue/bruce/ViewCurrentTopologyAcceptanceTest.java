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

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.junit.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import java.util.Properties;

/**
 * @author rklahn
 */
public class ViewCurrentTopologyAcceptanceTest {
    
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

    @Test public void testCurrentTopology() throws IOException, InterruptedException {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        PrintStream spawnOut = new PrintStream(outBytes);
        PrintStream spawnErr = new PrintStream(errBytes);

        PrintStream beforeOut = System.out;
        PrintStream beforeErr = System.err;

        // Set the System streams with our own
        System.setOut(spawnOut);
        System.setErr(spawnErr);

        // capture the current log4j configuration. The below call to main() is going to reconfigure log4j
        Properties log4jP = Log4jCapture.getLog4jConfig();

        // Run the main here
        com.netblue.bruce.admin.Main.main(new String[]{"-list", "-url", buildUrl("bruce_config")});

        // Restore original out and err
        System.setOut(beforeOut);
        System.setErr(beforeErr);

        // restore the original log4j configuration
        Log4jCapture.setLog4jConfig(log4jP);

        String stdout = outBytes.toString();
        String stderr = errBytes.toString();

	logger.debug("admin stdout:"+stdout);

        if (stderr.length() > 0) {
            Assert.fail("admin tool contains stderr output:" + stderr);
        }
        Assert.assertTrue(stdout.contains("Metadata for cluster [Cluster Un]"));
        Assert.assertTrue(stdout.contains("Master"));
        Assert.assertTrue(stdout.contains("Slaves"));
        Assert.assertTrue(stdout.contains("Name:\tCluster 0 - master\n" +
                "\tURL: jdbc:postgresql://localhost:5432/bruce_master?user=bruce&password=bruce\n"+
                "\tInclude table: ^(public\\..*|regextest\\..*|regextest_s2\\..*)$\n"));
        Assert.assertTrue(stdout.contains("Name:\tCluster 0 - slave 2\n" +
                "\tURL: jdbc:postgresql://localhost:5432/bruce_slave_2?user=bruce&password=bruce\n"+
                "\tInclude table: ^(public\\..*|regextest\\..*|regextest_s2\\..*)$\n"));
        Assert.assertTrue(stdout.contains("Name:\tCluster 0 - slave 1\n" +
                "\tURL: jdbc:postgresql://localhost:5432/bruce_slave_1?user=bruce&password=bruce\n"+
                "\tInclude table: ^(public\\..*|regextest\\..*|regextest_s2\\..*)$\n"));
    }

    private static final Logger logger = Logger.getLogger(ViewCurrentTopologyAcceptanceTest.class);
}
