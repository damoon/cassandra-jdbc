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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.cassandra.cql.ConnectionDetails;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PooledTest
{
	private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
	private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort() + ""));
	private static final String KEYSPACE = "testks";
	private static final String USER = "JohnDoe";
	private static final String PASSWORD = "secret";
	private static final String VERSION = "3.0.0";

	private static java.sql.Connection con = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
		Connection connection = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, "system"));
		Statement stmt = connection.createStatement();

		// Drop Keyspace
		String dropKS = String.format("DROP KEYSPACE \"%s\";", KEYSPACE);

		try
		{
			stmt.execute(dropKS);
		}
		catch (Exception e)
		{/* Exception on DROP is OK */
		}

		// Create KeySpace
		String createKS = String.format(
				"CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
				KEYSPACE);
		stmt.execute(createKS);
		
		stmt.close();
		
		connection.close();

		con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE));
	}
	
	@Before
	public void setUp() throws SQLException
	{
		Statement stmt = con.createStatement();
		
		// Create the target Column family
		stmt.execute("CREATE COLUMNFAMILY pooled_test (somekey text PRIMARY KEY, someInt int) ;");

		stmt.execute("UPDATE pooled_test SET someInt = '1' WHERE somekey = 'world'");
		stmt.execute("INSERT INTO pooled_test (somekey, someInt) VALUES ('hello', '1')");
		
		stmt.close();
	}
	
	@After
	public void tearDown() throws SQLException
	{
		con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE));
		Statement stmt = con.createStatement();
		stmt.execute("DROP COLUMNFAMILY pooled_test;");
		stmt.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		if (con != null) con.close();
	}


	@Test
	public void preparedStatement() throws Exception
	{
		PhysicalCassandraDataSource connectionPoolDataSource = new PhysicalCassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		PreparedStatement statement = connection.prepareStatement("SELECT someInt FROM pooled_test WHERE somekey = ?");
		statement.setString(1, "world");

		ResultSet resultSet = statement.executeQuery();
		assert resultSet.next();
		assert resultSet.getInt(1) == 1;
		assert resultSet.next() == false;
		resultSet.close();

		statement.close();
		connection.close();
	}

	@Test
	public void preparedStatementClose() throws Exception
	{
		PhysicalCassandraDataSource connectionPoolDataSource = new PhysicalCassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		PreparedStatement statement = connection.prepareStatement("SELECT someInt FROM pooled_test WHERE somekey = ?");
		statement.setString(1, "world");

		ResultSet resultSet = statement.executeQuery();
		assert resultSet.next();
		assert resultSet.getInt(1) == 1;
		assert resultSet.next() == false;
		resultSet.close();

		connection.close();
		
		assert statement.isClosed();
	}

	@Test
	public void statement() throws Exception
	{
		PhysicalCassandraDataSource connectionPoolDataSource = new PhysicalCassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		Statement statement = connection.createStatement();

		ResultSet resultSet = statement.executeQuery("SELECT someInt FROM pooled_test WHERE somekey = 'world'");
		assert resultSet.next();
		assert resultSet.getInt(1) == 1;
		assert resultSet.next() == false;
		resultSet.close();

		statement.close();
		connection.close();
	}

	@Test
	public void statementClosed() throws Exception
	{
		PhysicalCassandraDataSource connectionPoolDataSource = new PhysicalCassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		Statement statement = connection.createStatement();

		ResultSet resultSet = statement.executeQuery("SELECT someInt FROM pooled_test WHERE somekey = 'world'");
		assert resultSet.next();
		assert resultSet.getInt(1) == 1;
		assert resultSet.next() == false;
		resultSet.close();

		connection.close();
		
		assert statement.isClosed();
	}
}
