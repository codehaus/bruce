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

import com.netblue.bruce.*;
import com.netblue.bruce.cluster.*;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Builds all replication nodes found in the configuration database pointed to by <code>dataSource</code>. To build a
 * replication node, this class will install the replication schema (both master and slave tables) on each node, and
 * install the appropriate master/slave database triggers on all replicated tables
 *
 * @author lanceball
 * @version $Id$
 */
public class NodeBuilder
{
    public NodeBuilder(final Set<Node> nodes, final Options.SnapshotInitialization initSnapshots)
    {
        this.nodes = nodes;
        this.initOptions = initSnapshots;
        builder = new ReplicationDatabaseBuilder();
    }

    /**
     * Installs the replication schema and triggers on each node.  The schema is only installed on those nodes which do
     * not have a schema matching the current version.  Triggers are installed on all replicated tables for every node -
     * even if they exist already.
     */
    public void buildNodes() throws IOException, SQLException {
	HashSet<Cluster> clusters = new HashSet<Cluster>();

	for (Node node : nodes) {
	    for (Cluster cluster : node.getCluster()) {
		clusters.add(cluster);
	    }
	}

	for (Cluster cluster : clusters) {
	    Node master = cluster.getMaster();
	    prepareMaster(cluster);
	    // And only then can the slaves be prepared
	    for (Node node : nodes) {
		if (!master.getId().equals(node.getId())) {
		    prepareSlave(node,cluster);
		}
	    }
	}
    }

    private BasicDataSource prepareDatabase(final Node node) throws IOException, SQLException
    {
        final BasicDataSource nodeDataSource = DatabaseBuilder.makeDataSource(node.getUri(), null, null);
        if (!Version.isSameVersion(nodeDataSource))
        {
            builder.buildDatabase(nodeDataSource);
        }
        return nodeDataSource;
    }

    private void prepareSlave(final Node slave,Cluster c) throws SQLException, IOException
    {
        DataSource dataSource = prepareDatabase(slave);

        RegExReplicationStrategy strategy = new RegExReplicationStrategy(dataSource);
        Connection connection = dataSource.getConnection();
        ArrayList<String> tables = strategy.getTables(slave, null);
        Statement statement = connection.createStatement();
	createTransactionLogTable(statement,c.getId().toString());
	// Make sure we have at least one snapshot/transaction log
	LogSwitchHelper lst = new LogSwitchHelper(new BruceProperties(),dataSource,c);
	lst.newLogTable(statement);
        for (String table : tables)
        {
            String unqualifiedTableName = table.substring(table.lastIndexOf(".")+1);
            String denyTrigger = MessageFormat.format(DENY_ACCESS_TRIGGER_STMT, unqualifiedTableName, table);
            try
            {
                // The trigger may already be installed on the node so an exception is OK
                statement.execute(denyTrigger);
            }
            catch (SQLException e)
            {
                LOGGER.warn("Unexpected exception caught while preparing the slave node.");
                LOGGER.warn("Node: " + slave);
                LOGGER.warn("Table: " + table);
                LOGGER.warn("Exception: ", e);
            }
        }
        
        // Now that the slave has been initialized, we need to get the last processed snapshot
        Snapshot lastSnapshot = null;
        switch(initOptions)
        {
            case MASTER:
                Set<Cluster> clusters = slave.getCluster();
                for (Cluster aCluster : clusters)
                {
                    DataSource masterDataSource = prepareDatabase(aCluster.getMaster());
                    final Connection masterConnection = masterDataSource.getConnection();
                    final Snapshot snapshot = selectLastSnapshot(masterConnection.createStatement());
                    updateLastSnapshotForSlave(aCluster, snapshot, statement);
                    masterConnection.close();
                }
                break;
            case SLAVE:
                final Cluster cluster = slave.getCluster().iterator().next();
                updateLastSnapshotForSlave(cluster, selectLastSnapshot(statement), statement);
                break;
            case NONE:
                // nothing
        }
        statement.close();
        connection.close();
    }

    private void updateLastSnapshotForSlave(final Cluster cluster, final Snapshot lastSnapshot, final Statement statement) throws SQLException
    {
        String updateStatus = MessageFormat.format(UPDATE_STATUS_STATEMENT, lastSnapshot.getId(),
                                                   lastSnapshot.getMinXid().getLong(), lastSnapshot.getMaxXid().getLong(),
                                                   cluster.getId());
        LOGGER.info(updateStatus);
        statement.execute(updateStatus);
    }


    private void prepareMaster(final Cluster cluster) throws IOException
    {
	try { // Its possible that we pass thru this method more than once for a master. Ignore SQL errors.
	    Node master = cluster.getMaster();
	    DataSource dataSource = prepareDatabase(master);
	    
	    RegExReplicationStrategy strategy = new RegExReplicationStrategy(dataSource);
	    Connection connection = dataSource.getConnection();
	    ArrayList<String> tables = strategy.getTables(master, null);
	    Statement statement = connection.createStatement();
	    // Populate Master Node table with this clusters ID
	    String clusterIdS = cluster.getId().toString();
	    createMasterNodeTable(statement,clusterIdS);
	    // Create a currentlog table for this cluster
	    createTransactionLogTable(statement,clusterIdS);
	    for (String table : tables)
		{
		    // Trigger names can't be prefixed with a schema.  Let's get just the tableName
		    String unqualifiedTableName = table.substring(table.lastIndexOf(".")+1);
		    String txTrigger = MessageFormat.format(CREATE_TX_TRIGGER_STMT, unqualifiedTableName, table);
		    LOGGER.info(txTrigger);
		    statement.execute(txTrigger);
		    
		    String snapTrigger = MessageFormat.format(CREATE_SNAP_TRIGGER_STMT, unqualifiedTableName, table);
		    LOGGER.info(snapTrigger);
		    statement.execute(snapTrigger);
		}
	    
	    // Now check to see if we have any data in the snapshot view.  If not, create a row
	    // First, make sure at least one snapshot/transaction log exists
	    // Make sure we have at least one snapshot/transaction log
	    LogSwitchHelper lst = new LogSwitchHelper(new BruceProperties(),dataSource,cluster);
	    lst.newLogTable(statement);
	    // Make sure that we have at least one snapshot, so that this DB can safely be used as the data source
	    // of other nodes.
	    statement.execute("select bruce.logsnapshot()");
	    statement.close();
	    connection.close();
	} catch (SQLException e) {}
    }

    private Snapshot selectLastSnapshot(final Statement statement) throws SQLException
    {
        Snapshot snapshot = null;
        // Now get the last snapshot and return it
	// First we need to know what the clusterID is.
	ResultSet resultSet = statement.executeQuery("select cluster_id from masternode limit 1");
	Long clusterId = null;
	if (resultSet.next()) {
	    clusterId=resultSet.getLong("cluster_id");
	} 
	// Then we can build the query to get the snapshot
        resultSet = statement.executeQuery("select * from bruce.snapshotlog_"+clusterId.toString()+
					   " order by id desc limit 1");
        if (resultSet.next())
        {
            snapshot = new Snapshot(resultSet.getLong("id"),
				    new TransactionID(resultSet.getLong("min_xaction")),
				    new TransactionID(resultSet.getLong("max_xaction")),
				    resultSet.getString("outstanding_xactions"));
        }
        resultSet.close();
        return snapshot;
    }

    private void createMasterNodeTable(Statement s,String clusterId) throws SQLException {
 	try {
	s.execute("create table bruce.masternode (cluster_id int8 not null primary key)");
	s.execute("insert into bruce.masternode(cluster_id) values ("+clusterId+")");
	s.execute("grant select on bruce.masternode to public");
 	} catch (SQLException e) {} // OK if already exists
    }

    private void createTransactionLogTable(Statement s,String clusterId) throws SQLException {
 	try {
	s.execute("CREATE SEQUENCE bruce.currentlog_"+clusterId+"_id_seq "+
		  "      INCREMENT BY 1 NO MAXVALUE NO MINVALUE START WITH 1 CACHE 1");
	s.execute("CREATE SEQUENCE bruce.transactionlog_"+clusterId+"_rowseq "+
		  "      INCREMENT BY 1 NO MAXVALUE NO MINVALUE START WITH 1 CACHE 1");
	s.execute("grant all on bruce.transactionlog_"+clusterId+"_rowseq to public");
	s.execute("create sequence bruce.snapshotlog_"+clusterId+"_idseq "+
		  "      INCREMENT BY 1 NO MAXVALUE NO MINVALUE START WITH 1 CACHE 1");
	s.execute("grant all on bruce.snapshotlog_"+clusterId+"_idseq to public");
	s.execute("CREATE TABLE bruce.currentlog_"+clusterId+
		  "           ( id integer "+
		  "                DEFAULT nextval('bruce.currentlog_"+clusterId+"_id_seq'::regclass) "+
		  "                NOT NULL primary key, "+
		  "             create_time timestamp without time zone DEFAULT now() NOT NULL)");
	s.execute("GRANT select ON bruce.currentlog_"+clusterId+" TO public");
 	} catch (SQLException e) {} // OK if table already exists
    }

    private final Set<Node> nodes;
    private final ReplicationDatabaseBuilder builder;
    private Options.SnapshotInitialization initOptions;
    private static final Logger LOGGER = Logger.getLogger(NodeBuilder.class);
    private static final String CREATE_TX_TRIGGER_STMT = "CREATE TRIGGER {0}_tx AFTER INSERT OR DELETE OR UPDATE ON {1} FOR EACH ROW EXECUTE PROCEDURE logtransactiontrigger()";
    private static final String CREATE_SNAP_TRIGGER_STMT = "CREATE TRIGGER {0}_sn BEFORE INSERT OR DELETE OR UPDATE ON {1} FOR EACH STATEMENT EXECUTE PROCEDURE logsnapshottrigger()";
    private static final String DENY_ACCESS_TRIGGER_STMT = "CREATE TRIGGER {0}_deny BEFORE INSERT OR DELETE OR UPDATE ON {1} FOR EACH ROW EXECUTE PROCEDURE denyaccesstrigger()";
    private static final String UPDATE_STATUS_STATEMENT = "insert into bruce.slavesnapshotstatus (slave_xaction, master_id, master_min_xaction, master_max_xaction, update_time, clusterid) values (1, {0, number, #}, {1, number, #}, {2, number, #}, now(), {3, number, #})";
}
