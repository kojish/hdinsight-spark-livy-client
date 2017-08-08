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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *	LivyInteractiveClient is a class that submits remote job to Livy server with interactive mode.
 *	See https://msdn.microsoft.com/en-us/library/azure/mt613029.aspx for more detail.
 */
public class LivyInteractiveClient {
	private URL baseUri = null;
	private InteractiveSession session = null;
	private Authenticator auth = null;
	private ArrayList<StatementResult> array = null;
	private ExecutorService sessionThd = null;
	private ExecutorService stmtThd = null;
	
	/**
	 * Creates a LivyInteractiveClient with the given uri and auth info.
	 * @param uri URI that is https://{cluster-endpoint}/livy
	 * @param user Username for spark cluster 
	 * @param pwd Password for spark cluster
	 * @throws MalformedURLException
	 */
	public LivyInteractiveClient(String uri, String user, String pwd) throws MalformedURLException {
		baseUri = new URL(uri);
		auth = new BasicAuthenticator(user, pwd);
		array = new ArrayList<StatementResult>();
	}
		
	/**
	 * Gets all active interactive sessions
	 * @return All active sessions in JSON format.
	 * @throws IOException
	 */
	public String getActiveSessions() throws IOException {
		BufferedReader br = null;
		StringBuilder buf = new StringBuilder();
	
	    HttpURLConnection con = (HttpURLConnection)new URL(baseUri + "/sessions").openConnection();  
	    Authenticator.setDefault(auth);
	    con.setRequestMethod("GET");
	    con.setRequestProperty("Content-Type", "application/json");
	    
	    try {
	    	br = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));  
	    	String line = null;
	    	while ((line = br.readLine()) != null) {
	    		buf.append(line);
	    		buf.append("\r\n");  
	    	}
	    } finally {
	    	if(br != null)	br.close();
	    }
	
		return buf.toString();
	}
	
	/**
	 * Creates a new interactive session
	 * @param req InteractiveJobParameters object
	 * @return Session object that contains the session id returned from livy server
	 * @throws IOException
	 * @throws LivyException
	 */
	public Session createSession(InteractiveJobParameters req) throws IOException, LivyException {
		String data = JsonConverter.toJson(req);
        DataOutputStream os = null;
        BufferedReader br = null;

        HttpURLConnection con = (HttpURLConnection)new URL(baseUri +  "/sessions").openConnection();  
        Authenticator.setDefault(auth);
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Content-Length", String.valueOf(data.length()));
        con.setDoOutput(true);
        	
        os = new DataOutputStream(con.getOutputStream());
        try {
        	os.writeBytes(data);
        	int resp = con.getResponseCode();
        	if(resp != 200 && resp != 201) {
        		throw new LivyException("Invalid HTTP response code is returned. " + resp);
        	}
        	StringBuilder sb = new StringBuilder();
        	String line = "";
        	br = new BufferedReader(new InputStreamReader(con.getInputStream()));
        	while ((line = br.readLine()) != null) {
        		sb.append(line);
        		sb.append("\r\n");
        	}
        	String buf = sb.toString();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode root = mapper.readTree(buf);
        	session =  new InteractiveSession(Integer.valueOf(root.get("id").asText()));
System.out.println("ID: " + session.getId());
        } finally {
        	if(br != null) br.close();
        	if(os != null) os.close();
        }

		return session;
	}
	
	/**
	 * Creates a new interactive session with a listener for receiving a session status.
	 * @param conf InteractiveJobParameters object
	 * @param interval Interval time expressed in milliseconds for monitoring the status
	 * @param listener A listener to receive the status
	 * @return Session object that contains the session id returned from livy server
	 * @throws IOException
	 * @throws LivyException
	 */
	public Session createSession(InteractiveJobParameters req, final int interval, final SessionEventListener listener) throws IOException, LivyException {
		createSession(req);
	
		sessionThd = Executors.newSingleThreadExecutor();
		sessionThd.execute(new Runnable() {
			@Override
			public void run() {
				while(true) {
					try {
						boolean ret = listener.updateStatus(getSession());
						if(ret == false) {
							return;
						}
                		Thread.sleep(interval);
					} catch (IOException e1) {
						e1.printStackTrace();
						return;
                	} catch (InterruptedException e) {
                		e.printStackTrace();
                	}
                }
			}
		});
 
        return session;
	}

	/**
	 * Retrieves the current session information
	 * @return Session object
	 */
	public InteractiveSession getCurrentSession() {
		return session;
	}
	
	/**
	 * Gets the interactive session object.
	 * @return Interactive session
	 * @throws IOException
	 */
	public InteractiveSession getSession() throws IOException {
		BufferedReader br = null;
		int status = Session.STARTING;
		
		if(session.getState() == Session.DEAD) return session;
		
		HttpURLConnection con = (HttpURLConnection)new URL(baseUri + "/sessions/" + session.getId()).openConnection();  
		Authenticator.setDefault(auth);
		con.setRequestMethod("GET");
		con.setRequestProperty("Content-Type", "application/json");

	    StringBuilder sb = new StringBuilder();
	    String line = null;
	    try {
	    	br = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
	    	while ((line = br.readLine()) != null) {
	    		sb.append(line);
	    		sb.append("\r\n");
	    	}
	    } catch(FileNotFoundException ex) {
	    	throw new IOException(ex);
	    } finally {
	    	if(br != null) br.close();
	    }

	    String buf = sb.toString();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(buf);
    	String state = root.get("state").asText();
    	if(state != null) {
    		if(state.equals("starting") == true) status = Session.STARTING;
    		else if(state.equals("idle") == true) status = Session.IDLE;
    		else if(state.equals("dead") == true) status = Session.DEAD;
    		else if(state.equals("error") == true) status = Session.ERROR;
    		else status = Session.ERROR;
    		session.setState(status);
    	}else{
    		session.setState(Session.ERROR);
    	}
    	if(root.get("appId") != null) session.setAppId(root.get("appId").asText());
    	if(root.get("appInfo") != null) session.setAppInfo(root.get("appInfo").asText());
    	if(root.get("log") != null) session.setLog(root.get("log").asText());
    	if(root.get("proxyUser") != null) session.setProxyUser(root.get("proxyUser").asText());
    	if(root.get("kind") != null) {
			SessionKind tmp = SessionKind.getEnum(root.get("kind").asText());
			session.setKind(tmp);
			//session.setKind(SessionKind.getEnum(root.get("ind").asText()));
		}
    	
		return session;
	}

	/**
	 * Submits the statement(s) to livy server.
	 * @param statement Set the statement(s). Use semicolon to send the multiple statements.
	 * @throws LivyException
	 * @throws IOException
	 */
	public void submitStatement(final String statement) throws LivyException, IOException {

        String data = "{\"code\" : \"" + statement + "\"}";
        DataOutputStream os = null;
        BufferedReader br = null;
 
		HttpURLConnection con = (HttpURLConnection)new URL(baseUri + "/sessions/" + session.getId() + "/statements").openConnection();  
		Authenticator.setDefault(auth);
	    con.setRequestMethod("POST");
	    con.setRequestProperty("Content-Type", "application/json");
	    con.setRequestProperty("Content-Length", String.valueOf(data.length()));
	    con.setDoOutput(true);
        os = new DataOutputStream(con.getOutputStream());
        try {
        	os.writeBytes(data);
        	int resp = con.getResponseCode();
        	if(resp != 200 && resp != 201) {
        		throw new LivyException("Invalid HTTP response code is returned. " + resp);
        	}
            
        	StringBuilder sb = new StringBuilder();
        	String line = "";
        	br = new BufferedReader(new InputStreamReader(con.getInputStream()));
        	while ((line = br.readLine()) != null) {
        		sb.append(line);
        		sb.append("\r\n");      
        	}
        } finally {
        	if(br != null) br.close();
        	if(os != null) os.close();
        }
	}
	
	/**
	 * Submits a statement to spark cluster
	 * @param statement Set the statement(s). Use semicolon to send the multiple statements.
	 * @param interval Interval time expressed in milliseconds for monitoring the status
	 * @param listener A listener object to get the statement's result.
	 * @throws LivyException
	 * @throws IOException
	 */
	public void submitStatement(final String statement, final int interval, final StatementResultListener listener) throws LivyException, IOException {
		submitStatement(statement);
	
		stmtThd = Executors.newSingleThreadExecutor();
		stmtThd.execute(new Runnable() {
			@Override
			public void run(){

            	int current = array.size();

				ObjectMapper mapper = new ObjectMapper();
				JsonNode root = null;

                while(true) {
					String buf;
					try {
						buf = getStatementResult();
					  	System.out.print("BUF: " + buf);
					  	root = mapper.readTree(buf);
						int total = Integer.valueOf(root.get("total_statements").asText());
						StatementResults ret = JsonConverter.toObject(StatementResults.class, buf);
						System.out.println("RET: " + ret.total_statements);
						List<Statements> stt = ret.statements;
						stt.forEach(s -> {
							if(s.id != current) return;
					  		if(s.output == null) return;
							if(!s.state.equals(StatementResult.STATE_AVAILABLE)) return;

							StatementResult sr;
							if(s.output.status.equals("error")) {
/*								System.out.println("ID: " + s.id );
								System.out.println("STATE: " + s.state);
								System.out.println("STATUS: " + s.output.status);
								System.out.println("EXECUTION_COUNT :" + s.output.execution_count);
*/
								sr = new StatementResult(s.id, s.state, s.output.execution_count, s.output.status, "", statement);
							} else {
/*								System.out.println("ID: " + s.id );
								System.out.println("STATE: " + s.state);
								System.out.println("STATUS: " + s.output.status);
								System.out.println("EXECUTION_COUNT :" + s.output.execution_count);
								System.out.println("Text: " + s.output.data.text);
*/
								sr = new StatementResult(s.id, s.state, s.output.execution_count, s.output.status, s.output.data.text, statement);
							}
							array.add(sr);
							listener.update(sr);
						});

              	  		if(array.size() == current + 1) {
              		  		break;
              	  		}
              	  } catch (IOException e1) {
              		  e1.printStackTrace();
              		  return;
              	  }

              	  try {
              		  Thread.sleep(interval);
              	  } catch (InterruptedException e) {
              		  e.printStackTrace();
              	  }

                }
           }
        });
	}
	
	/**
	 * Gets the result of statements
	 * See https://msdn.microsoft.com/en-us/library/azure/mt613032.aspx
	 * @return Result of statement in JSON format
	 * @throws IOException
	 */
	public String getStatementResult() throws IOException {
		BufferedReader br = null;
		String line = null;
		StringBuilder sb = new StringBuilder();
			
		HttpURLConnection con = (HttpURLConnection)new URL(baseUri + "/sessions/" + session.getId() + "/statements").openConnection();  
		Authenticator.setDefault(auth);
	    con.setRequestMethod("GET");
	    con.setRequestProperty("Content-Type", "application/json");
		          
	    br = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));  
	    try {
	    	while ((line = br.readLine()) != null) {
	    		sb.append(line);
	    		sb.append("\r\n");  
	    	}
	    } finally {
	    	if(br != null) br.close();
	    }
		return sb.toString();
	}
	
	/**
	 * Deletes the interactive session
	 * @throws IOException
	 */
	public void deleteSession() throws IOException {
		BufferedReader br = null;
		
		try {
			HttpURLConnection con = (HttpURLConnection)new URL(baseUri + "/sessions/" + session.getId()).openConnection();  
			Authenticator.setDefault(auth);
	    	con.setRequestMethod("DELETE");
	    	con.setRequestProperty("Content-Type", "application/json");
	    	br = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));  
	    	String line = null;
	    	StringBuilder buf = new StringBuilder();
	    	while ((line = br.readLine()) != null) {
	    		buf.append(line);
	    		buf.append("\r\n");  
	    	}
	    } finally {
	    	if(br != null) br.close();
	    	array.clear();
	    	session.reset();
	    	if(sessionThd != null && sessionThd.isShutdown() == false)	sessionThd.shutdown();
	    	if(stmtThd != null && stmtThd.isShutdown() == false)	stmtThd.shutdown();
	    }
	}
}

class StatementResults {
	public int total_statements;
	public List<Statements> statements;
}

class Statements {
	public int id;
	public String state;
	public Output output;
}

class Output {
	public String status;
	public int execution_count;
	public String ename;
	public String evalue;
	public String[] traceback;
	public Data data;
}

class Data {
	@JsonProperty("text/plain")
	public String text;
}
