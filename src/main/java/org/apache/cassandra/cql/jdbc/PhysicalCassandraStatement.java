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

import static org.apache.cassandra.cql.jdbc.Utils.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra statement: implementation class for {@link PreparedStatement}.
 */

public class PhysicalCassandraStatement extends AbstractStatement implements CassandraStatement, Comparable<Object>
{
    private static final Logger logger = LoggerFactory.getLogger(PhysicalCassandraStatement.class);
    /**
     * The connection.
     */
    protected PhysicalCassandraConnection connection;

    /**
     * The cql.
     */
    protected String cql;

    protected int fetchDirection = ResultSet.FETCH_FORWARD;

    protected int fetchSize = 0;

    protected int maxFieldSize = 0;

    protected int maxRows = 0;

    protected int resultSetType = CassandraResultSet.DEFAULT_TYPE;

    protected int resultSetConcurrency = CassandraResultSet.DEFAULT_CONCURRENCY;

    protected int resultSetHoldability = CassandraResultSet.DEFAULT_HOLDABILITY;

    protected CassandraResultSet currentResultSet = null;

    protected int updateCount = -1;

    protected boolean escapeProcessing = true;

    PhysicalCassandraStatement(PhysicalCassandraConnection con)
    {
        this(con, null);
    }

    PhysicalCassandraStatement(PhysicalCassandraConnection con, String cql)
    {
        this.connection = con;
        this.cql = cql;
    }

    PhysicalCassandraStatement(PhysicalCassandraConnection con, String cql, int resultSetType, int resultSetConcurrency,
                       int resultSetHoldability)
    {
    	this(con, cql);

        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
    }

    public void addBatch(String arg0) throws SQLRecoverableException, SQLFeatureNotSupportedException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(NO_BATCH);
    }

    protected final void checkNotClosed() throws SQLRecoverableException
    {
        if (isClosed()) throw new SQLRecoverableException(WAS_CLOSED_STMT);
    }

    public void clearBatch() throws SQLRecoverableException, SQLFeatureNotSupportedException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(NO_BATCH);
    }

    public void clearWarnings() throws SQLRecoverableException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    public void close()
    {
        if (connection != null)
        {
            connection.removeStatement(this);
            connection = null;
            cql = null;
        }
    }
        

    private void doExecute(String cql, ConsistencyLevel consistencyLevel) throws SQLException
    {
        try
        {
            if (logger.isTraceEnabled()) logger.trace("CQL: "+ cql);
            
            resetResults();
            CqlResult rSet = connection.execute(cql, consistencyLevel);

            switch (rSet.getType())
            {
                case ROWS:
                    currentResultSet = new CassandraResultSet(this, rSet);
                    break;
                case INT:
                    updateCount = rSet.getNum();
                    break;
                case VOID:
                    updateCount = 0;
                    break;
            }
        }
        catch (InvalidRequestException e)
        {
            throw new SQLSyntaxErrorException(e.getWhy()+"\n'"+cql+"'",e);
        }
        catch (UnavailableException e)
        {
            throw new SQLNonTransientConnectionException(NO_SERVER, e);
        }
        catch (TimedOutException e)
        {
            throw new SQLTransientConnectionException(e);
        }
        catch (SchemaDisagreementException e)
        {
            throw new SQLRecoverableException(SCHEMA_MISMATCH);
        }
        catch (TException e)
        {
            throw new SQLNonTransientConnectionException(e);
        }

    }

    public boolean execute(String query) throws SQLException
    {
        checkNotClosed();
        doExecute(query, ConsistencyLevel.ONE);
        return !(currentResultSet == null);
    }

    public boolean execute(String query, ConsistencyLevel consistencyLevel) throws SQLException
    {
        checkNotClosed();
        doExecute(query, consistencyLevel);
        return !(currentResultSet == null);
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkNotClosed();

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS))
            throw new SQLSyntaxErrorException(BAD_AUTO_GEN);

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS) throw new SQLFeatureNotSupportedException(NO_GEN_KEYS);

        return execute(sql);
    }

    public int[] executeBatch() throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException(NO_BATCH);
    }

    public CassandraResultSet executeQuery(String query) throws SQLException
    {
        checkNotClosed();
        doExecute(query, ConsistencyLevel.ONE);
        if (currentResultSet == null)
            throw new SQLNonTransientException(NO_RESULTSET);
        return currentResultSet;
    }

    public CassandraResultSet executeQuery(String query, ConsistencyLevel consistencyLevel) throws SQLException
    {
        checkNotClosed();
        doExecute(query, consistencyLevel);
        if (currentResultSet == null)
            throw new SQLNonTransientException(NO_RESULTSET);
        return currentResultSet;
    }

    public int executeUpdate(String query) throws SQLException
    {
        checkNotClosed();
        doExecute(query, ConsistencyLevel.ONE);
        if (currentResultSet != null)
            throw new SQLNonTransientException(NO_UPDATE_COUNT);
        return updateCount;
    }

    public int executeUpdate(String query, ConsistencyLevel consistencyLevel) throws SQLException
    {
        checkNotClosed();
        doExecute(query, consistencyLevel);
        if (currentResultSet != null)
            throw new SQLNonTransientException(NO_UPDATE_COUNT);
        return updateCount;
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkNotClosed();

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS))
            throw new SQLFeatureNotSupportedException(BAD_AUTO_GEN);

        return executeUpdate(sql);
    }

    public Connection getConnection() throws SQLRecoverableException
    {
        checkNotClosed();
        return (Connection) connection;
    }

    public int getFetchDirection() throws SQLRecoverableException
    {
        checkNotClosed();
        return fetchDirection;
    }

    public int getFetchSize() throws SQLRecoverableException
    {
        checkNotClosed();
        return fetchSize;
    }

    public int getMaxFieldSize() throws SQLRecoverableException
    {
        checkNotClosed();
        return maxFieldSize;
    }

    public int getMaxRows() throws SQLRecoverableException
    {
        checkNotClosed();
        return maxRows;
    }

    public boolean getMoreResults() throws SQLRecoverableException
    {
        checkNotClosed();
        resetResults();
        // in the current Cassandra implementation there are never MORE results
        return false;
    }

    public boolean getMoreResults(int current) throws SQLException
    {
        checkNotClosed();

        switch (current)
        {
            case CLOSE_CURRENT_RESULT:
                resetResults();
                break;

            case CLOSE_ALL_RESULTS:
            case KEEP_CURRENT_RESULT:
                throw new SQLFeatureNotSupportedException(NO_MULTIPLE);

            default:
                throw new SQLSyntaxErrorException(String.format(BAD_KEEP_RSET, current));
        }
        // in the current Cassandra implementation there are never MORE results
        return false;
    }

    public int getQueryTimeout()
    {
        // the Cassandra implementation does not support timeouts on queries
        return 0;
    }

    public CassandraResultSet getResultSet() throws SQLRecoverableException
    {
        checkNotClosed();
        return currentResultSet;
    }

    public int getResultSetConcurrency() throws SQLRecoverableException
    {
        checkNotClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getResultSetHoldability() throws SQLRecoverableException
    {
        checkNotClosed();
        // the Cassandra implementations does not support commits so this is the closest match
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public int getResultSetType() throws SQLRecoverableException
    {
        checkNotClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public int getUpdateCount() throws SQLRecoverableException
    {
        checkNotClosed();
        return updateCount;
    }

    public SQLWarning getWarnings() throws SQLRecoverableException
    {
        checkNotClosed();
        return null;
    }

    public boolean isClosed()
    {
        return connection == null;
    }

    public boolean isPoolable() throws SQLRecoverableException
    {
        checkNotClosed();
        return false;
    }

    public boolean isWrapperFor(Class<?> iface)
    {
        return false;
    }

    protected final void resetResults()
    {
        currentResultSet = null;
        updateCount = -1;
    }

    public void setEscapeProcessing(boolean enable) throws SQLRecoverableException
    {
        checkNotClosed();
        // the Cassandra implementation does not currently look at this
        escapeProcessing = enable;
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        checkNotClosed();

        if (direction == ResultSet.FETCH_FORWARD || direction == ResultSet.FETCH_REVERSE || direction == ResultSet.FETCH_UNKNOWN)
        {
            if ((getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) && (direction != ResultSet.FETCH_FORWARD))
                throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
            fetchDirection = direction;
        }
        else throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }


    public void setFetchSize(int size) throws SQLException
    {
        checkNotClosed();
        if (size < 0) throw new SQLSyntaxErrorException(String.format(BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    public void setMaxFieldSize(int arg0) throws SQLRecoverableException
    {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
    }

    public void setMaxRows(int arg0) throws SQLRecoverableException
    {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
    }

    public void setPoolable(boolean poolable) throws SQLRecoverableException
    {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (false)
    }

    public void setQueryTimeout(int arg0) throws SQLRecoverableException
    {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (0)
    }

    public <T> T unwrap(Class<T> iface) throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    public int compareTo(Object target)
    {
        if (this.equals(target)) return 0;
        if (this.hashCode()< target.hashCode()) return -1;
        else return 1;
    }
}
