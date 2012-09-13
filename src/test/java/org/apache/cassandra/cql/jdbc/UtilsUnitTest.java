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

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
import org.junit.BeforeClass;
import org.junit.Test;

public class UtilsUnitTest
{

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {}

    @Test
    public void testParseURL() throws Exception
    {
        String happypath = "jdbc:cassandra://localhost:9170/Keyspace1?version=2.0.0&transportFactory=org.example.FooTransportFactory";
        Properties props = Utils.parseURL(happypath);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(Utils.TAG_CQL_VERSION));
        assertEquals("org.example.FooTransportFactory", props.getProperty(Utils.TAG_TRANSPORT_FACTORY));
        
        String noport = "jdbc:cassandra://localhost/Keyspace1?version=2.0.0&transportFactory=org.example.FooTransportFactory";
        props = Utils.parseURL(noport);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9160", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(Utils.TAG_CQL_VERSION));
        assertEquals("org.example.FooTransportFactory", props.getProperty(Utils.TAG_TRANSPORT_FACTORY));
        
        String noversion = "jdbc:cassandra://localhost:9170/Keyspace1?transportFactory=org.example.FooTransportFactory";
        props = Utils.parseURL(noversion);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertNull(props.getProperty(Utils.TAG_CQL_VERSION));
        assertEquals("org.example.FooTransportFactory", props.getProperty(Utils.TAG_TRANSPORT_FACTORY));
        
        String nokeyspaceonly = "jdbc:cassandra://localhost:9170?version=2.0.0&transportFactory=org.example.FooTransportFactory";
        props = Utils.parseURL(nokeyspaceonly);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertNull(props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(Utils.TAG_CQL_VERSION));
        assertEquals("org.example.FooTransportFactory", props.getProperty(Utils.TAG_TRANSPORT_FACTORY));
        
        String nokeyspaceorver = "jdbc:cassandra://localhost:9170?transportFactory=org.example.FooTransportFactory";
        props = Utils.parseURL(nokeyspaceorver);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertNull(props.getProperty(Utils.TAG_DATABASE_NAME));
        assertNull(props.getProperty(Utils.TAG_CQL_VERSION));
        assertEquals("org.example.FooTransportFactory", props.getProperty(Utils.TAG_TRANSPORT_FACTORY));
        
        String nokeyspaceorverortransport = "jdbc:cassandra://localhost:9170";
        props = Utils.parseURL(nokeyspaceorverortransport);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertNull(props.getProperty(Utils.TAG_DATABASE_NAME));
        assertNull(props.getProperty(Utils.TAG_CQL_VERSION));
        assertNull(props.getProperty(Utils.TAG_TRANSPORT_FACTORY));
        
        String notransportonly = "jdbc:cassandra://localhost:9170/Keyspace1?version=2.0.0";
        props = Utils.parseURL(notransportonly);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9170", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(Utils.TAG_CQL_VERSION));
        assertNull(props.getProperty(Utils.TAG_TRANSPORT_FACTORY));
    }
  
    @Test
    public void testCreateSubName() throws Exception
    {
        String happypath = "jdbc:cassandra://localhost:9170/Keyspace1?version=2.0.0&transportFactory=org.example.FooTransportFactory";
        Properties props = Utils.parseURL(happypath);
        
        String result = Utils.createSubName(props);
        assertEquals(happypath, Utils.PROTOCOL+result);
    }
}
