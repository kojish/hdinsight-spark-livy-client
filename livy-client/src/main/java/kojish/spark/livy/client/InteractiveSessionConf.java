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
package kojish.spark.livy.client;

/**
 * Configuration for an interactive session. Used for setting various parameters when creating a new session.
 * The configuration data is listed here: https://msdn.microsoft.com/en-us/library/azure/mt613027.aspx
 * Note: Currently, all settings are not supported.
 */
public class InteractiveSessionConf extends SessionConf {
	
	public static String KIND_SPARK = "spark";
	public static String KIND_PYSPARK = "pyspark";
	public static String KIND_SPARKR = "sparkr";
	private String kind = KIND_SPARK;
	private StringBuffer conf = new StringBuffer();
	
	/**
	 * Creates a session configuration for interactive mode. 
	 * @param ssnKind The session kind
	 */	
	public InteractiveSessionConf(String ssnKind) {
		if(ssnKind == null) return;
		kind = ssnKind;
	}

	/**
	 * Get the configuration for interactive session.
	 * @return configuration
	 */
	@Override
	public String getConf() {
		conf.append("{\"kind\" : \"");
		conf.append(kind);
		conf.append("\"");
		if(jars != null && jars.length() > 0) {
			conf.append(", \"jars\" : [");
			conf.append(jars);
			conf.append("\"]");
		}
		if(appName != null && appName.length() > 0) {
			conf.append(", \"name\" : " + "\"" + appName + "\"");
		}
		conf.append("}");
		
		return conf.toString();
	}
}
