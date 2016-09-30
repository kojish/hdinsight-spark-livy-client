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

import java.net.Authenticator;
import java.net.PasswordAuthentication;

/**
 * A class to be used for authentication against Spark cluster
 */
public class BasicAuthenticator extends Authenticator {
	private String password;
	private String username;

	/**
	 * Constructor to create an object
	 * @param user
	 * @param pwd
	 */
	public BasicAuthenticator(String user, String pwd){
		password = pwd;
		username = user;
	}

	protected PasswordAuthentication getPasswordAuthentication(){
		return new PasswordAuthentication(username, password.toCharArray());
	}
}
