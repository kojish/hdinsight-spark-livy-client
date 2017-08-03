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
 * Represents the result of statement.
 *
 */
public class StatementResult {

	public static final String STATE_RUNNING = "running";
	public static final String STATE_AVAILABLE = "available";
	
	private int id = 0;
	private int execution_count = 0;
	private String state = StatementResult.STATE_RUNNING;
	private String status = "ok";
	private String output = "null";
	private String statement = null;
	
	public StatementResult(int num, String st, int ec, String stts, String out, String stmt) {
		id = num;
		execution_count = ec;
		state = st;
		status = stts;
		output = out;
		statement = stmt;
	}

	public int getId() { return id; }
	public int getExecutionCount() { return execution_count; }
	public String getState() { return state; }
	public String getStatus() { return status; }
	public String getOutput() { return output; }
	public String getStatement() { return statement; }

	public void setState(String st) { state = st; }
	public void setStatus(String stts) { status = stts; }
	public void setOutput(String out) { output = out; }
	public void setExecutionCount(int ec) { execution_count = ec; }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof StatementResult){
			StatementResult cmp = (StatementResult)obj;
			if(this.id == cmp.getId())
			return true;	  
		}
		return false;
	}

    @Override
    public int hashCode() {
    	int hash = 7;
	    hash = 17 * hash + (this.statement != null ? this.statement.hashCode() : 0);
	    return hash;
	}
}
