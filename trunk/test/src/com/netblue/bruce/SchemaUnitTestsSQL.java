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

import com.netblue.bruce.admin.DatabaseBuilder;
import org.apache.log4j.Logger;

public class SchemaUnitTestsSQL extends DatabaseBuilder {
    public String[] getSqlStrings() {
        return testSQL;
    }

    private static final String[] testSQL = {
	"DROP TABLE test2",
	"DROP TABLE test1",
	"DROP SCHEMA regextest cascade",
	"CREATE SCHEMA regextest",
	"CREATE TABLE regextest.blue(c_int integer)",
	"CREATE TABLE regextest.red(c_int integer)",
	"CREATE TABLE regextest.green(c_int integer)",
	"CREATE TABLE regextest.orange(c_int integer)",
	"DROP SCHEMA regextest_s2 cascade",
	"CREATE SCHEMA regextest_s2",
	"CREATE TABLE regextest_s2.blue2(c_int integer)",
	"CREATE TABLE regextest_s2.red2(c_int integer)",
	"CREATE TABLE regextest_s2.green2(c_int integer)",
	"CREATE TABLE regextest_s2.orange2(c_int integer)",
	"CREATE TABLE public.test2 (id integer primary key,c_bytea bytea,c_text text,c_int integer)",
	"CREATE TABLE public.test1 (id serial NOT NULL primary key,c_bytea bytea,c_text text,c_int integer)",
    };
}