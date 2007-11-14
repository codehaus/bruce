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
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.*;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.sql.SQLException;
import java.io.IOException;

/**
 * @author lanceball
 * @version $Id$
 */

public class OneAndOnlyOneMasterPerClusterAcceptanceTest {
    @BeforeClass public static void setupTestClass() throws SQLException, InterruptedException {
	TestDatabaseHelper.createNamedTestDatabase("bruce_master");
	TestDatabaseHelper.createNamedTestDatabase("bruce_slave_1");
	TestDatabaseHelper.createNamedTestDatabase("bruce_slave_2");
        // Create a cluster. Master only. No tables in replication
        String[] args = new String[]{"-data",
                                     TestDatabaseHelper.getTestDataDir() + "/admin-test-setup.xml",
                                     "-initnodeschema",
                                     "-loadschema",
                                     "-operation", "CLEAN_INSERT",
                                     "-url", TestDatabaseHelper.buildUrl("bruce_master")};
        com.netblue.bruce.admin.Main.main(args);
	mDS=TestDatabaseHelper.createDataSource(TestDatabaseHelper.buildUrl("bruce_master"));
    }

    @AfterClass public static void teardownAfterClass() throws SQLException {
	if (mDS != null) {
	    mDS.close();
	}
    }

    @Test public void testOneMaster() throws IOException {
	ByteArrayOutputStream adminBAOS = new ByteArrayOutputStream();

        // capture the current log4j configuration. We are about to modify it, and want to go back to the original 
	// later.
        Properties log4jP = Log4jCapture.getLog4jConfig();

	logger.warn("We expect the admin tool to have a 'duplicate key' problem. Its exactly what we are testing "+
		    "for. Ignore any ERRORs or WARNINGs to that effect");

	Logger.getRootLogger().addAppender(new WriterAppender(new PatternLayout(),adminBAOS));

        // Run the main here
	com.netblue.bruce.admin.Main.main(new String[]{"-data",
						       TestDatabaseHelper.getTestDataDir()+
						       "/one-and-only-one-master-add.xml",
						       "-operation", "INSERT",
						       "-url", TestDatabaseHelper.buildUrl("bruce_master")});
        // restore the original log4j configuration
        Log4jCapture.setLog4jConfig(log4jP);

	logger.debug("admin tool output: "+adminBAOS.toString());

        Assert.assertTrue("Unexpected output from admin tool:  " + adminBAOS.toString(), 
			  adminBAOS.toString().contains("org.postgresql.util.PSQLException: " +
							"ERROR: duplicate key violates unique constraint " +
							"\"yf_cluster_master_node_id_key\""));
    }

    private static final Logger logger = Logger.getLogger(OneAndOnlyOneMasterPerClusterAcceptanceTest.class);
    private static BasicDataSource mDS;
}
