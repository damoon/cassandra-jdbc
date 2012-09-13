/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.cql.jdbc;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.security.auth.login.LoginException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import static org.apache.cassandra.cql.jdbc.Utils.*;

/**
 * Implementation class for {@link Connection}.
 */
class CassandraConnection extends AbstractCassandraConnection implements Connection
{

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

    public static final int DB_MAJOR_VERSION = 1;
    public static final int DB_MINOR_VERSION = 1;
    public static final String DB_PRODUCT_NAME = "Cassandra";
    public static final String DEFAULT_CQL_VERSION = "2.0.0";

    public static Compression defaultCompression = Compression.GZIP;

    private final boolean autoCommit = true;

    private final int transactionIsolation = Connection.TRANSACTION_NONE;

    /**
     * Connection Properties
     */
    private Properties connectionProps;

    /**
     * Client Info Properties (currently unused)
     */
    private Properties clientInfo = new Properties();

    /**
     * Set of all Statements that have been created by this connection
     */
    private Set<Statement> statements = new ConcurrentSkipListSet<Statement>();

    private Cassandra.Client client;
    private TTransport transport;

    protected long timeOfLastFailure = 0;
    protected int numFailures = 0;
    protected String username = null;
    protected String url = null;
    String currentKeyspace;
    ColumnDecoder decoder;


    /**
     * Instantiates a new CassandraConnection.
     */
    public CassandraConnection(Properties props) throws SQLException
    {
        connectionProps = (Properties)props.clone();
        clientInfo = new Properties();
        url = PROTOCOL + createSubName(props);
        try
        {
            String host = props.getProperty(TAG_SERVER_NAME);
            int port = Integer.parseInt(props.getProperty(TAG_PORT_NUMBER));
            currentKeyspace = props.getProperty(TAG_DATABASE_NAME,"system");
            username = props.getProperty(TAG_USER);
            String password = props.getProperty(TAG_PASSWORD);
            String version = props.getProperty(TAG_CQL_VERSION);

            TSocket socket = new TSocket(host, port);
            ITransportFactory transportFactory = getTransportFactory(props);
            connectionProps.put(Utils.TAG_TRANSPORT_FACTORY, transportFactory.getClass().getName());
            transport = transportFactory.openTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new Cassandra.Client(protocol);

            if (username != null)
            {
                Map<String, String> credentials = new HashMap<String, String>();
                credentials.put("username", username);
                if (password != null) credentials.put("password", password);
                AuthenticationRequest areq = new AuthenticationRequest(credentials);
                client.login(areq);
            }
            
            if (version != null)
            {
                client.set_cql_version(version);
                connectionProps.setProperty(TAG_ACTIVE_CQL_VERSION, version);
            }
            
            decoder = new ColumnDecoder(client.describe_keyspaces());
                    
            client.set_keyspace("system");

            client.set_keyspace(currentKeyspace);

            Object[] args = {host, port,currentKeyspace,(version==null) ? DEFAULT_CQL_VERSION: version};
            logger.info("Connected to {}:{} using Keyspace {} and CQL version {}",args);                       
        }
        catch (InvalidRequestException e)
        {
            throw new SQLSyntaxErrorException(e);
        }
        catch (TException e)
        {
            throw new SQLNonTransientConnectionException(e);
        }
        catch (AuthenticationException e)
        {
            throw new SQLInvalidAuthorizationSpecException(e);
        }
        catch (AuthorizationException e)
        {
            throw new SQLInvalidAuthorizationSpecException(e);
        }
        catch (LoginException e)
        {
            throw new SQLInvalidAuthorizationSpecException(e);
        }
    }

    private ITransportFactory getTransportFactory(Properties props) throws TException
    {
        
        String transportFactoryClassName = props.getProperty(Utils.TAG_TRANSPORT_FACTORY);
        if (null != transportFactoryClassName)
        {
            try
            {
                return (ITransportFactory) Class.forName(transportFactoryClassName).newInstance();
            }
            catch (Exception e) 
            {
                throw new TException( String.format("Unable to initialise specified TransportFactory %s", transportFactoryClassName), e);
            }
        }
        else
        {
            return new TFramedTransportFactory();
        }
    }
    
    
    private final void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    /**
     * On close of connection.
     */
    public synchronized void close() throws SQLException
    {
        // close all statements associated with this connection upon close
        for (Statement statement : statements)
            statement.close();
        statements.clear();
        
        if (isConnected())
        {
            // then disconnect from the transport                
            disconnect();
        }
    }

    public void commit() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public Statement createStatement() throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this);
        statements.add(statement);
        return statement;
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency);
        statements.add(statement);
        return statement;
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency, resultSetHoldability);
        statements.add(statement);
        return statement;
    }

    public boolean getAutoCommit() throws SQLException
    {
        checkNotClosed();
        return autoCommit;
    }

    public Properties getConnectionProps()
    {
        return connectionProps;
    }

    public String getCatalog() throws SQLException
    {
        // This implementation does not support the catalog names so null is always returned if the connection is open.
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
        return null;
    }

    public Properties getClientInfo() throws SQLException
    {
        checkNotClosed();
        return clientInfo;
    }

    public String getClientInfo(String label) throws SQLException
    {
        checkNotClosed();
        return clientInfo.getProperty(label);
    }

    public int getHoldability() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are really no commits in Cassandra so no boundary...
        return CassandraResultSet.DEFAULT_HOLDABILITY;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        checkNotClosed();
        return new CassandraDatabaseMetaData(this);
    }

    public int getTransactionIsolation() throws SQLException
    {
        checkNotClosed();
        return transactionIsolation;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    public synchronized boolean isClosed() throws SQLException
    {

        return !isConnected();
    }

    public boolean isReadOnly() throws SQLException
    {
        checkNotClosed();
        return false;
    }

    public boolean isValid(int timeout) throws SQLException
    {
        checkNotClosed();
        if (timeout < 0) throw new SQLTimeoutException(BAD_TIMEOUT);

        // this needs to be more robust. Some query needs to be made to verify connection is really up.
        return !isClosed();
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException
    {
        return false;
    }

    public String nativeSQL(String sql) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no distinction between grammars in this implementation...
        // so we are just return the input argument
        return sql;
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        checkNotClosed();
        PreparedStatement statement = new CassandraPreparedStatement(this, sql);
        statements.add(statement);
        return statement;
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void rollback() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        checkNotClosed();
        if (!autoCommit) throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setCatalog(String arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no catalog name to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setClientInfo(Properties props) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        if (props != null) clientInfo = props;
    }

    public void setClientInfo(String key, String value) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        clientInfo.setProperty(key, value);
    }

    public void setHoldability(int arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no holdability to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setReadOnly(boolean arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is all connections are read/write in the Cassandra implementation...
        // so we are "silently ignoring" the request
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        checkNotClosed();
        if (level != Connection.TRANSACTION_NONE) throw new SQLFeatureNotSupportedException(NO_TRANSACTIONS);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    /**
     * Execute a CQL query.
     *
     * @param queryStr    a CQL query string
     * @param compression query compression to use
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    protected CqlResult execute(String queryStr, Compression compression) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        currentKeyspace = determineCurrentKeyspace(queryStr, currentKeyspace);

        try
        {
            return client.execute_cql_query(Utils.compressQuery(queryStr, compression), compression);
        }
        catch (TException error)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            throw error;
        }
    }

    /**
     * Execute a CQL query using the default compression methodology.
     *
     * @param queryStr a CQL query string
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    protected CqlResult execute(String queryStr)
              throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return execute(queryStr, defaultCompression);
    }

    protected CqlResult execute(int itemId, List<ByteBuffer> values)
              throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        try
        {
            return client.execute_prepared_cql_query(itemId, values);
        }
        catch (TException error)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            throw error;
        }
    }
    
    protected CqlPreparedResult prepare(String queryStr, Compression compression)throws InvalidRequestException, TException
    {
        try
        {
            return client.prepare_cql_query(Utils.compressQuery(queryStr, compression), compression);
        }
        catch (TException error)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            throw error;
        }
    }
    
    protected CqlPreparedResult prepare(String queryStr) throws InvalidRequestException, TException
    {
        try
        {
            return prepare(queryStr, defaultCompression);
        }
        catch (TException error)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            throw error;
        }
    }
    
    /**
     * Remove a Statement from the Open Statements List
     */
    protected boolean removeStatement(Statement statement)
    {
        return statements.remove(statement);
    }
    
    /**
     * Shutdown the remote connection
     */
    protected void disconnect()
    {
        transport.close();
    }

    /**
     * Connection state.
     */
    protected boolean isConnected()
    {
        return transport.isOpen();
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("CassandraConnection [connectionProps=");
        builder.append(connectionProps);
        builder.append("]");
        return builder.toString();
    }    
}
