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
import static org.junit.Assert.*;
import org.junit.*;

import java.util.Random;

public class BrucePropertiesTest
{

    private static final Logger logger = Logger.getLogger(BrucePropertiesTest.class);

    // Test BruceProperties.getIntProperty for the case where the key is not set.
    // We should expect back the default integer
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Test public void testGIPKeyNotSet() {
        BruceProperties p = new BruceProperties();
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            int rndI = r.nextInt();
            int propI = p.getIntProperty("key.value.that.does.not.exist", rndI);
            assertEquals(rndI, propI);
        }
    }

    // Test BruceProperties.getIntProperty for the case where the key is set,
    // but its set to something that will not convert to integer
    // We should expect back the default integer
    @Test public void testGIPKeySetNotInt() {
        BruceProperties p = new BruceProperties();
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            int rndI = r.nextInt();
            String key = (new Integer(rndI)).toString();
            p.setProperty(key, "something not an Integer");
            int propI = p.getIntProperty(key, rndI);
            assertEquals(rndI, propI);
        }
    }

    // Test BruceProperties.getIntProperty for the case where the key is set,
    // and its set to something that will convert to an integer
    // We should expect back the integer that the property is set to.
    @Test public void testGIPKeySetInt() {
        BruceProperties p = new BruceProperties();
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            int rndDefaultI = r.nextInt();
            int rndI = r.nextInt();
            String key = (new Integer(rndI)).toString();
            p.setProperty(key, key);
            int propI = p.getIntProperty(key, rndDefaultI);
            assertEquals(rndI, propI);
        }
    }
}
