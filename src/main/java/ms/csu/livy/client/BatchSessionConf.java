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
 * Configuration for a batch session. Used for setting various parameters when creating a new session.
 * The configuration data is listed here: https://msdn.microsoft.com/en-us/library/azure/mt613020.aspx 
 * Note: Currently, minimum settings are supported.
 */
public class BatchSessionConf extends SessionConf {
	private String file = "";
	private String className = "";
	private StringBuffer conf = new StringBuffer();
	
	/**
	 * Constructor 
	 * @param file Path to the batch job's jar
	 * @param cls The class name of the main class
	 */	
	public BatchSessionConf(String fileName, String cls) {
		file = fileName;
		className = cls;
	}
	
	/**
	 * Get the configuration for batch session.
	 * @return configuration
	 */	
	@Override
	public String getConf() {
		conf.append("{\"file\" : \"");
		conf.append(file);
		conf.append("\",");
		conf.append("\"className\" : \"");
		conf.append(className);
		conf.append("\"");
		if(jars != null && jars.length() > 0) {
			conf.append(", \"jars\" : [");
			conf.append(jars);
			conf.append("\"]");
		}
		conf.append("}");
		
		return conf.toString();	
	}
}
