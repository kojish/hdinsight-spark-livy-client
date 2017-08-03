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
package delsh.livy;

/**
 * Represents a session to be used for an interactive mode.
 */
abstract public class Session {
	// For interactive
	public static final int STARTING = 0;
	public static final int IDLE = 1;
	public static final int ERROR = 2;
	public static final int DEAD = 3;
	// For batch
	public static final int RUNNING = 4;
	public static final int SUCCESS = 5;
	
	public static final int BUSY = 6;
	public static final int NOT_STARTED = 7;
	
	private int session_id = Session.STARTING;
	private int state = Session.STARTING;
	private String appId = null;
	private String appInfo = null;
	private String log = null;
	
	public Session(int id) {
		session_id = id;
	}
	
	public int getId() {
		return session_id;
	}
	
	public int getState() {
		return state;
	}

	public void setState(int st) {
		state = st;
	}
	
	public String getAppId() {
		return appId;
	}

	public void setAppId(String id) {
		appId = id;
	}
	
	public String getAppInfo() {
		return appInfo;
	}

	public void setAppInfo(String info) {
		appInfo = info;
	}
	
	public String getLog() {
		return log;
	}

	public void setLog(String log) {
		this.log = log;
	}
	
	public void reset() {
		session_id = Session.STARTING;
		state = Session.STARTING;
	}
}
