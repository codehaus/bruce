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

public class SchemaUnitTestsTriggers extends DatabaseBuilder {
    public String[] getSqlStrings() {
        return testSQL;
    }

    private static final String[] testSQL = {
	"CREATE TRIGGER test1_t AFTER INSERT OR DELETE OR UPDATE ON public.test1 FOR EACH ROW "+
	"                       EXECUTE PROCEDURE bruce.logtransactiontrigger()",
	"CREATE TRIGGER test1_s BEFORE INSERT OR DELETE OR UPDATE ON public.test1 FOR EACH STATEMENT "+
	"                       EXECUTE PROCEDURE bruce.logsnapshottrigger()",
	"CREATE TRIGGER test2_t BEFORE INSERT OR DELETE OR UPDATE ON public.test2 FOR EACH ROW "+
	"                       EXECUTE PROCEDURE bruce.denyaccesstrigger()"
    };
}