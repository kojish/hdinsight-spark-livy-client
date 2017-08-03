/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ms.csu.livy.client;

/**
 * A class to set the configurations that is commonly used for both batch and interactive mode.
 */
abstract public class SessionConf {
	
	protected String jars = null;
	protected String pyFiles = null;		// Not implemented yet
	protected String files = null;			// Not implemented yet
	protected String proxyUser  =null;		// Not implemented yet
	protected String driverMemory = null;	// Not implemented yet
	protected int driverCores = 0;			// Not implemented yet
	protected String executorMemory = null;	// Not implemented yet
	protected int executorCores = 0;		// Not implemented yet
	protected int numExecutors = 0;			// Not implemented yet
	protected String archives = null;		// Not implemented yet
	protected String appName = null;		// Not implemented yet

	/**
	 * Set file path of jars to be placed on the java classpath
	 * @param The path for Jar files
	 */
	public void setJars(String[] path) {
		if(path == null) return;
		if(path.length < 1) return;
		
		StringBuffer buf = new StringBuffer();
		// Jars are comma-separated if two or more paths are set.
		for(int cnt=0; cnt<path.length; cnt++) {
			buf.append("\"");
			buf.append(path[cnt]);
			if(cnt < path.length-1) buf.append("\",");
		}
		jars = buf.toString();
	}

	/**
	 * Set a name for your application
	 * @param Name of the application
	 */
	public void setAppName(String name) {
		appName = name;
	}

	/**
	 * Get the configuration. The differences of configuration between batch and interactive mode needs to be implemented by subclass.
	 * @return configuration
	 */
	abstract public String getConf();
}
