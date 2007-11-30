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
import org.apache.log4j.Logger;
import org.junit.*;
import static com.netblue.bruce.admin.Version.*;
import static com.netblue.bruce.TestDatabaseHelper.*;
import static org.junit.Assert.*;

import java.sql.SQLException;

/**
 * Tests {@link com.netblue.bruce.admin.Version}
 * @author lanceball
 * @version $Id$
 */
public class VersionTest {

    @Test public void testVersionNumber() {
        int major = getMajorVersionNumber();
        int minor = getMinorVersionNumber();
        int patch = getPatchVersionNumber();
        String name = getVersionName();
	
        assertEquals("Unexpected major version number", 1, major);
        assertEquals("Unexpected minor version number", 5, minor);
        assertEquals("Unexpected patch version number", 0, patch);
        assertEquals("Unexpected patch version number", "Replication 1.5 release", name);
    }

    @Test public void testIsSameVersion() throws SQLException,InterruptedException {
	createNamedTestDatabase("bruce");
	// Create the cluster. Master with no slaves.
	com.netblue.bruce.admin.Main.main(new String[]{
		"-data",getTestDataDir()+"/master-only-empty.xml",
		"-initnodeschema",
		"-loadschema",
		"-operation","CLEAN_INSERT",
		"-url",buildUrl("bruce")
	    });
	BasicDataSource bds = createDataSource(buildUrl("bruce"));
        assertTrue("Schema and code version conflict", isSameVersion(bds));
        assertTrue("Schema and code version conflict", isSameVersion(1, 5, 0, "Replication 1.5 release"));
	bds.close();
    }

    @Test public void testIsSameVersionForNonExistentDatabase() throws SQLException {
	BasicDataSource bds = createDataSource(buildUrl("I_DONT_EXIST"));
        assertFalse("Version.isSameVersion() returned true for a non-existent DB",isSameVersion(bds));
	bds.close();
    }
}
