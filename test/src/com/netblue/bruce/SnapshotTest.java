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

import org.apache.log4j.Logger;
import org.junit.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.TreeSet;

public class SnapshotTest {

    @Test public void testCompareTo() {
        assertTrue(s1.compareTo(s2) < 0);
        assertTrue(s2.compareTo(s1) > 0);
        assertTrue(s1.compareTo(s1) == 0);
    }

    @Test public void testEquals() {
        assertTrue(s1.equals(s1));
        assertFalse(s1.equals(s2));
    }

    @Test public void testConstructors() {
        // A snapshot that should be equal, but using a different constructor
        Snapshot s1a = new Snapshot(new Long(4L),
				    new TransactionID(4),
                                    new TransactionID(5),
                                    new TreeSet<TransactionID>());
        assertTrue(s1.equals(s1a));
    }

    @Test public void testTransactionIDLT() {
        Snapshot s5 = new Snapshot(new Long(10L),new TransactionID(10), new TransactionID(20), "11,12,13");
        assertTrue(s5.transactionIDLT(new TransactionID(4))); // Less than min
        assertTrue(s5.transactionIDLT(new TransactionID(10))); // Equal to min
        assertFalse(s5.transactionIDLT(new TransactionID(20))); // Equal to max
        assertFalse(s5.transactionIDLT(new TransactionID(21))); // Greater than max
        assertFalse(s5.transactionIDLT(new TransactionID(11))); // Between, on list
        assertTrue(s5.transactionIDLT(new TransactionID(14))); // Between, not on list
        s5 = new Snapshot(new Long(10L),new TransactionID(10), new TransactionID(20), "10,20");
        assertFalse(s5.transactionIDLT(new TransactionID(10))); // equal to min, on list
        assertFalse(s5.transactionIDLT(new TransactionID(20))); // equal to max, on list
    }

    @Test public void testTransactionIDGE() {
        Snapshot s5 = new Snapshot(new Long(10L),new TransactionID(10), new TransactionID(20), "11,12,13");
        assertFalse(s5.transactionIDGE(new TransactionID(4))); // Less than min
        assertFalse(s5.transactionIDGE(new TransactionID(10))); // Equal to min
        assertTrue(s5.transactionIDGE(new TransactionID(20))); // Equal to max
        assertTrue(s5.transactionIDGE(new TransactionID(21))); // Greater than max
        assertTrue(s5.transactionIDGE(new TransactionID(11))); // Between, on list
        assertFalse(s5.transactionIDGE(new TransactionID(14))); // Between, not on list
        s5 = new Snapshot(new Long(10L),new TransactionID(10),new TransactionID(20), "10,20");
        assertTrue(s5.transactionIDGE(new TransactionID(10))); // equal to min, on list
        assertTrue(s5.transactionIDGE(new TransactionID(20))); // equal to max, on list
    }

    private static final Logger logger = Logger.getLogger(SnapshotTest.class.getName());
    private final Snapshot s1 = new Snapshot(new Long(4L), new TransactionID(4), new TransactionID(5), "");
    private final Snapshot s2 = new Snapshot(new Long(6L), new TransactionID(6), new TransactionID(7), "");
}
