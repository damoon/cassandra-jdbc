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

import static org.apache.cassandra.cql.jdbc.Utils.HOST_REQUIRED;
import static org.apache.cassandra.cql.jdbc.Utils.NOT_SUPPORTED;
import static org.apache.cassandra.cql.jdbc.Utils.NO_INTERFACE;
import static org.apache.cassandra.cql.jdbc.Utils.PROTOCOL;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_CQL_VERSION;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_DATABASE_NAME;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_PASSWORD;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_PORT_NUMBER;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_SERVER_NAME;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_USER;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_TRANSPORT_FACTORY;
import static org.apache.cassandra.cql.jdbc.Utils.createSubName;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class CassandraDataSource implements DataSource
{

    static
    {
        try
        {
            Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected static final String description = "Cassandra Data Source";

    protected String serverName;

    protected int    portNumber = 9160;

    protected String databaseName;

    protected String user;

    protected String password;

    protected String version = null;
    
    protected String transportFactory = null;

    public CassandraDataSource(String host, int port, String keyspace, String user, String password, String version)
    {
        if (host != null) setServerName(host);
        if (port != -1) setPortNumber(port);
        if (version != null) setVersion(version);
        setDatabaseName(keyspace);
        setUser(user);
        setPassword(password);
    }

    public String getDescription()
    {
        return description;
    }

    public String getServerName()
    {
        return serverName;
    }

    public void setServerName(String serverName)
    {
        this.serverName = serverName;
    }

    public String getVersion()
    {
        return version;
    }

    public void setVersion(String version)
    {
        this.version = version;
    }

    public int getPortNumber()
    {
        return portNumber;
    }

    public void setPortNumber(int portNumber)
    {
        this.portNumber = portNumber;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public void setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setTransportFactory(String transportFactory)
    {
        this.transportFactory = transportFactory;
    }
    
    public Connection getConnection() throws SQLException
    {
        return getConnection(null, null);
    }

    public Connection getConnection(String user, String password) throws SQLException
    {
        Properties props = new Properties();
        
        this.user = user;
        this.password = password;
        
        if (this.serverName!=null) props.setProperty(TAG_SERVER_NAME, this.serverName);
        else throw new SQLNonTransientConnectionException(HOST_REQUIRED);
        props.setProperty(TAG_PORT_NUMBER, ""+this.portNumber);
        if (this.databaseName!=null) props.setProperty(TAG_DATABASE_NAME, this.databaseName);
        if (user!=null) props.setProperty(TAG_USER, user);
        if (password!=null) props.setProperty(TAG_PASSWORD, password);
        if (this.version != null) props.setProperty(TAG_CQL_VERSION, version);
        if(this.transportFactory != null) props.setProperty(TAG_TRANSPORT_FACTORY, this.transportFactory);

        String url = PROTOCOL+createSubName(props);
        return DriverManager.getConnection(url, props);
    }

    public int getLoginTimeout() throws SQLException
    {
        return DriverManager.getLoginTimeout();
    }

    public PrintWriter getLogWriter() throws SQLException
    {
        return DriverManager.getLogWriter();
    }

    public void setLoginTimeout(int timeout) throws SQLException
    {
        DriverManager.setLoginTimeout(timeout);
    }

    public void setLogWriter(PrintWriter writer) throws SQLException
    {
        DriverManager.setLogWriter(writer);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface.isAssignableFrom(getClass());
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }  
    
    public Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
    	throw new SQLFeatureNotSupportedException(String.format(NOT_SUPPORTED));
    }
}
