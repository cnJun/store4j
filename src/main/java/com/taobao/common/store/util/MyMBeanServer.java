/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package com.taobao.common.store.util;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

/**
 * 便于注册MBean的工具类
 * 
 * @author dogun (yuexuqiang at gmail.com)
 *
 */
public final class MyMBeanServer {
	private static final Logger log = Logger.getLogger(MyMBeanServer.class);
	private MBeanServer mbs = null;

	private static MyMBeanServer me = new MyMBeanServer();
	private MyMBeanServer() {
	    //创建MBServer
		String hostName = null;
		 try {
             InetAddress localhost = InetAddress.getLocalHost();

             hostName        = localhost.getHostName();
         } catch (UnknownHostException e) {
             hostName        = "localhost";
         }
    	try {
    		boolean useJmx = Boolean.parseBoolean(System.getProperty("store4j.useJMX", "false"));
    		if (useJmx) {
    			mbs = ManagementFactory.getPlatformMBeanServer();
	    		int port = Integer.parseInt(System.getProperty("store4j.rmi.port", "6669"));
	    		String rmiName = System.getProperty("store4j.rmi.name", "store4j");
	    		Registry reg = null;
	    		try {
	    			reg = LocateRegistry.getRegistry(port);
	    			reg.list();
	    		}catch (Exception e) {
	    			reg = null;
	    		}
	    		if (null == reg) {
	    			reg = LocateRegistry.createRegistry(port);
	    		}
	    		reg.list();
	    		String serverURL = "service:jmx:rmi://"+hostName+"/jndi/rmi://"+hostName+":"+port+"/" + rmiName;
	    		JMXServiceURL url = new JMXServiceURL(serverURL);
				final JMXConnectorServer connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
				connectorServer.start();
				Runtime.getRuntime().addShutdownHook(new Thread() {
					@Override
					public void run() {
						try {
							System.err.println("JMXConnector stop");
							connectorServer.stop();
						} catch (IOException e) {
							log.error("stop error", e);
						}
					}
				});
				log.warn("jmx url: " + serverURL);
    		}
		} catch (Exception e) {
			log.error("create MBServer error", e);
		}
	}
	
	/**
	 * 获得MBeanServer
	 * @return MyMBeanServer
	 */
	public static MyMBeanServer getInstance() {
		return me;
	}
	
	/**
	 * 注册一个MBean
	 * @param o
	 * @param name
	 */
	public void registMBean(Object o, String name) {
		//注册MBean
		if (null != mbs) {
			try {
				mbs.registerMBean(o, new ObjectName(o.getClass().getPackage().getName() + 
						":type=" + o.getClass().getSimpleName() 
						+ (null == name ? (",id=" + o.hashCode()) : (",name=" + name + "-" + o.hashCode()))));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
