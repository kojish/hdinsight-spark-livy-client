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
 * Represents a session data for an interactive mode.
 */
public class InteractiveSession extends Session {

	private String proxyUser = null;
	private String kind = SessionConf.KIND_SPARK;

	/**
	 * Creates an interactive session object.
	 * @param id session id
	 */
	public InteractiveSession(int id) {
		super(id);
	}

	/**
	 * Sets a proxy user name.
	 * @param user proxy user
	 */
	public void setProxyUser(String user) {
		proxyUser = user;
	}

	/**
	 * Obtains the proxy user
	 * @return proxy user
	 */
	public String getProxyUser() {
		return proxyUser;
	}

	/**
	 * Sets a kind
	 * @param kind Session kind
	 */
	public void setKind(String kind) {
		this.kind = kind;
	}

	/**
	 * Gets the session kind
	 * @return Session kind
	 */
	public String getKind() {
		return kind;
	}
}