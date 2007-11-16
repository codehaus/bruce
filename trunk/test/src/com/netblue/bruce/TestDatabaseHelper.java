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
import static org.junit.Assert.*;

import java.io.File;
import java.sql.*;
import java.util.StringTokenizer;

/**
 * Helper class for tests that deal with the database (which is almost all of them).
 *
 * @author lanceball
 * @version $Id$
 */
public class TestDatabaseHelper {
    /** 
     * Create a named database, dropping it if nessasary. Create a bruce schema within it.
     *
     * @param dBName Name of database to create.
     */
    public static void createNamedTestDatabase(String dBName) throws SQLException, InterruptedException {
        Connection connection = getAdminDataSource().getConnection();
	try { // Guarentee the connection gets closed
	    final Statement statement = connection.createStatement();
	    try { // Guarentee the statement gets closed.
		connection.setAutoCommit(true);

		// Make sure no connections to named database exist
		for (int retry=3;retry>0;retry--) {
		    System.runFinalization();
		    System.gc();
		    ResultSet rs = executeQueryAndLog(statement,
						      "select count(*) from pg_stat_activity where datname = '"+
						      dBName+"'");
		    assertTrue("Error retreving database connection count",rs.next());
		    if (rs.getInt(1) == 0) break;
		    logger.info(rs.getInt(1)+" database connections to "+dBName+" exist. Will wait and retry "+
				retry+" more times");
		    Thread.sleep(5000L); // 5 seconds
		}
	    
		// Drop the database. SQLException here is OK, probably means the database does not
		// already exist
		try {
		    logger.debug("Creating "+dBName+" database");
		    executeAndLog(statement, "drop database " + dBName);
		} catch (SQLException e) {}

		// Create the database.
		executeAndLog(statement,"create database "+dBName);
	    } finally {
		statement.close();
	    }
	} finally {
	    connection.close();
	}
    }

    public static BasicDataSource getAdminDataSource() {
	if (adminDataSource == null) {
            logger.debug("Creating admin DataSource");
            adminDataSource = createDataSource(properties.getProperty(POSTGRESQL_ADMIN_URL_KEY));
        }
        return adminDataSource;
    }

    public static BasicDataSource createDataSource(String URI) {
	BasicDataSource dataSource = new BasicDataSource();
	dataSource.setDriverClassName("org.postgresql.Driver");
	dataSource.setUrl(URI);
	return dataSource;
    }

    /**
     * Returns the directory location for test data sets.
     *
     * @return The directory path
     */
    public static String getTestDataDir() {
        return properties.getProperty(TEST_DATA_DIR_KEY,TEST_DATA_DIR_DEFAULT);
    }

    public static String baseUrlFromUrl(String URL) {
	StringTokenizer st = new StringTokenizer(URL,"?");
	if (st.hasMoreTokens()) {
	    return st.nextToken();
	} else {
	    return URL;
	}
    }

    public static String buildBaseUrl(String dbName) {
	return JDBC_POSTGRESQL_PREFIX +
	    properties.getProperty(POSTGRESQL_PORT_KEY,POSTGRESQL_PORT_DEFAULT)+"/"+
	    dbName;
    }

    public static String buildUrl(String dbName) {
	return buildBaseUrl(dbName)+"?user=bruce&password=bruce";
    }

    public static boolean executeAndLog(Statement s, String cmd) throws SQLException {
        logger.debug(cmd);
        return s.execute(cmd);
    }

    public static ResultSet executeQueryAndLog(Statement s, String cmd) throws SQLException {
        logger.debug(cmd);
        return s.executeQuery(cmd);
    }

    public static ResultSet executePreparedQueryAndLog(PreparedStatement ps) throws SQLException {
	logger.debug(ps);
	return ps.executeQuery();
    }

    public static boolean executePreparedAndLog(PreparedStatement ps) throws SQLException {
	logger.debug(ps);
	return ps.execute();
    }

    private static final Logger logger = Logger.getLogger(TestDatabaseHelper.class);
    private static final String POSTGRESQL_ADMIN_URL_KEY  = "postgresql.adminURL";
    private static final String TEST_DATA_DIR_KEY = "dir.test.data";
    private static final String TEST_DATA_DIR_DEFAULT = "test/data";
    private static final String POSTGRESQL_PORT_KEY      = "postgresql.port";
    private static final String POSTGRESQL_PORT_DEFAULT  = "5432";
    private static final String JDBC_POSTGRESQL_PREFIX = "jdbc:postgresql://localhost:";
    private static final BruceProperties properties = new BruceProperties();

    private static BasicDataSource adminDataSource;
}
