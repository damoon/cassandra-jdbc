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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledCassandraDataSource implements CassandraDataSource, ConnectionEventListener
{
	static final int CONNECTION_IS_VALID_TIMEOUT = 1;

	private static final int MAX_POOL_SIZE = 64;

	protected static final String NOT_SUPPORTED = "the Cassandra implementation does not support this method";

	private static final Logger logger = LoggerFactory.getLogger(PooledCassandraDataSource.class);

	private static final int MAX_CHECKOUTS = 512;

	// 6 minutes
	private static final long MAX_MILLIS_AGE = 360000;
	
	private LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
	
	private ThreadPoolExecutor threadPool = new ThreadPoolExecutor(6, 6, 60, TimeUnit.SECONDS, workQueue);

	private PhysicalCassandraDataSource dataSource;

	private ConcurrentLinkedQueue<PooledCassandraConnection> freeConnections = new ConcurrentLinkedQueue<PooledCassandraConnection>();

	private ConcurrentLinkedQueue<PooledCassandraConnection> usedConnections = new ConcurrentLinkedQueue<PooledCassandraConnection>();
	
	public PooledCassandraDataSource(PhysicalCassandraDataSource dataSource) throws SQLException
	{
		this.dataSource = dataSource;
		for (int i=0; i<MAX_POOL_SIZE / 4; i++)
		{
			threadPool.execute(new ConnectionPoolFiller(freeConnections, dataSource, this));
		}
	}

	@Override
	public CassandraConnection getConnection() throws SQLException
	{
		PooledCassandraConnection connection = null;

		do
		{
			if (freeConnections.isEmpty())
			{		
				if (workQueue.size() < MAX_POOL_SIZE / 8)
				{
					for (int i=0; i<MAX_POOL_SIZE / 8; i++)
					{
						threadPool.execute(new ConnectionPoolFiller(freeConnections, dataSource, this));
					}
				}
				if (freeConnections.isEmpty())
				{
			 		synchronized (freeConnections)
			 		{
						try
						{
							freeConnections.wait(10);
						}
						catch (InterruptedException e)
						{
							throw new SQLException(e);
						}
			 		}
				}
			}
			connection = freeConnections.poll();
			
			if (connection != null && getLifetime(connection) > MAX_MILLIS_AGE) {
				connection.close();
				connection = null;
			}
		}
		while (connection == null);
		
		usedConnections.add(connection);

		return new ManagedConnection(connection);
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException
	{
		throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
	}

	@Override
	public void connectionClosed(ConnectionEvent event)
	{
		PooledCassandraConnection connection = (PooledCassandraConnection) event.getSource();
		usedConnections.remove(connection);

		if (freeConnections.size() < MAX_POOL_SIZE)
		{
			threadPool.execute(new ConnectionPoolRejoiner(freeConnections, connection));
		}
		else
		{
			connection.close();
		}
	}

	@Override
	public synchronized void connectionErrorOccurred(ConnectionEvent event)
	{
		PooledCassandraConnection connection = (PooledCassandraConnection) event.getSource();

		if (!(event.getSQLException() instanceof SQLRecoverableException) || !isStillUseable(connection))
		{
			connection.getConnection().close();
			usedConnections.remove(connection);
		}
	}

	public synchronized void close() throws InterruptedException
	{
		try
		{
			threadPool.shutdown();
			threadPool.awaitTermination(10l, TimeUnit.SECONDS);
		}
		finally
		{
			closePooledConnections(usedConnections);
			closePooledConnections(freeConnections);
		}
	}

	private void closePooledConnections(ConcurrentLinkedQueue<PooledCassandraConnection> usedConnections)
	{
		for (PooledCassandraConnection connection : usedConnections)
		{
			connection.close();
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
	
	private static class ConnectionPoolRejoiner implements Runnable {
		
		private volatile ConcurrentLinkedQueue<PooledCassandraConnection> freeConnections;
		
		private PooledCassandraConnection connection;
       
        public ConnectionPoolRejoiner(ConcurrentLinkedQueue<PooledCassandraConnection> freeConnections, PooledCassandraConnection connection)
        {
            this.connection = connection;
            this.freeConnections = freeConnections;
        }
       
        @Override
        public void run() {
           if (isStillUseable(connection) && freeConnections.size() < MAX_POOL_SIZE)
           {
        	   freeConnections.add(connection);
        	   synchronized (freeConnections)
        	   {
        		   freeConnections.notifyAll();
        	   }
           }
           else
           {
        	   connection.close();
           }
        }
    }
	
	private static class ConnectionPoolFiller implements Runnable {
		
		private volatile ConcurrentLinkedQueue<PooledCassandraConnection> freeConnections;
		
		private PhysicalCassandraDataSource dataSource;
		
		private PooledCassandraDataSource pooledDataSource;
		       
		public ConnectionPoolFiller(ConcurrentLinkedQueue<PooledCassandraConnection> freeConnections, PhysicalCassandraDataSource dataSource, PooledCassandraDataSource pooledDataSource)
        {
            this.dataSource = dataSource;
            this.freeConnections = freeConnections;
            this.pooledDataSource = pooledDataSource;
        }
       
        @Override
        public void run()
        {
	        try
			{
	        	PooledCassandraConnection connection = dataSource.getPooledConnection();
				connection.addConnectionEventListener(pooledDataSource);
				freeConnections.add(connection);
			}
			catch (SQLException e)
			{
				logger.error("could not create a new connection", e);
			}
        }
    }

	private static boolean isStillUseable(PooledCassandraConnection connection)
	{
		try
		{
			return connection.getOuthandedCount() < MAX_CHECKOUTS
					&& getLifetime(connection) < MAX_MILLIS_AGE
					&& connection.getConnection().isValid(CONNECTION_IS_VALID_TIMEOUT);
		}
		catch (SQLTimeoutException e)
		{
			logger.error("this should not happen", e);
			return false;
		}
	}

	private static long getLifetime(PooledCassandraConnection connection)
	{
		return System.currentTimeMillis()- connection.getCreationMillistime();
	}

	// Method not annotated with @Override since getParentLogger() is a new method
	// in the CommonDataSource interface starting with JDK7 and this annotation
	// would cause compilation errors with JDK6.
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        return dataSource.getParentLogger();
    }
}
