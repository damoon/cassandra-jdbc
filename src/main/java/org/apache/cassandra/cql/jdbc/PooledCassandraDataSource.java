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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.util.HashSet;
import java.util.Set;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.DataSource;
import javax.sql.PooledConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledCassandraDataSource implements DataSource, ConnectionEventListener
{
	static final int CONNECTION_IS_VALID_TIMEOUT = 5;

	private static final int MIN_POOL_SIZE = 4;

	protected static final String NOT_SUPPORTED = "the Cassandra implementation does not support this method";

	private static final Logger logger = LoggerFactory.getLogger(PooledCassandraDataSource.class);

	private CassandraDataSource dataSource;

	private volatile Set<PooledCassandraConnection> freeConnections = new HashSet<PooledCassandraConnection>();

	private volatile Set<PooledCassandraConnection> usedConnections = new HashSet<PooledCassandraConnection>();

	public PooledCassandraDataSource(CassandraDataSource dataSource) throws SQLException
	{
		this.dataSource = dataSource;
	}

	@Override
	public synchronized Connection getConnection() throws SQLException
	{
		PooledCassandraConnection pooledConnection;

		if (freeConnections.isEmpty())
		{
			pooledConnection = dataSource.getPooledConnection();
			pooledConnection.addConnectionEventListener(this);
		}
		else
		{
			pooledConnection = freeConnections.iterator().next();
			freeConnections.remove(pooledConnection);
		}

		usedConnections.add(pooledConnection);
		return new ManagedConnection(pooledConnection);
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException
	{
		throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
	}

	@Override
	public synchronized void connectionClosed(ConnectionEvent event)
	{
		PooledCassandraConnection connection = (PooledCassandraConnection) event.getSource();
		usedConnections.remove(connection);
		int freeConnectionsCount = freeConnections.size();
		try
		{
			if (freeConnectionsCount < MIN_POOL_SIZE && !connection.getConnection().isClosed())
			{
				freeConnections.add(connection);
			}
			else
			{
				try
				{
					connection.close();
				}
				catch (SQLException e)
				{
					logger.error(e.getLocalizedMessage());
				}
			}
		}
		catch (SQLException e)
		{
			logger.error(e.getLocalizedMessage());
		}
	}

	@Override
	public synchronized void connectionErrorOccurred(ConnectionEvent event)
	{
		PooledCassandraConnection connection = (PooledCassandraConnection) event.getSource();

		try
		{
			if (!(event.getSQLException() instanceof SQLRecoverableException)
					|| !connection.getConnection().isValid(CONNECTION_IS_VALID_TIMEOUT))
			{
				try
				{
					connection.getConnection().close();
				}
				catch (SQLException e)
				{
					logger.error(e.getLocalizedMessage());
				}
				usedConnections.remove(connection);
			}
		}
		catch (SQLTimeoutException e)
		{
			logger.error(e.getLocalizedMessage());
		}
	}

	public synchronized void close()
	{
		closePooledConnections(usedConnections);
		closePooledConnections(freeConnections);
	}

	private void closePooledConnections(Set<PooledCassandraConnection> usedConnections2)
	{
		for (PooledConnection connection : usedConnections2)
		{
			try
			{
				connection.close();
			}
			catch (SQLException e)
			{
				logger.error(e.getLocalizedMessage());
			}
		}
	}

	@Override
	public int getLoginTimeout()
	{
		return dataSource.getLoginTimeout();
	}

	@Override
	public void setLoginTimeout(int secounds)
	{
		dataSource.setLoginTimeout(secounds);
	}

	@Override
	public PrintWriter getLogWriter()
	{
		return dataSource.getLogWriter();
	}

	@Override
	public void setLogWriter(PrintWriter writer)
	{
		dataSource.setLogWriter(writer);
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0)
	{
		return dataSource.isWrapperFor(arg0);
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException
	{
		return dataSource.unwrap(arg0);
	}
}
