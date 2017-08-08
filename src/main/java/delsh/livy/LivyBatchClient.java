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
import java.util.concurrent.ExecutorService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * LivyBatchClient is a class that submits spark job to Livy server with batch mode.
 * See https://msdn.microsoft.com/en-us/library/mt613033.aspx
 */
public class LivyBatchClient {
	
	private URL baseUri = null;
	private BatchSession session = null;
	private Authenticator auth = null;

	/**
	 * Creates a LivyBatchClient with the given uri and auth info.
	 * @param uri URI that is https://{cluster-endpoint}/livy
	 * @param user Username for spark cluster 
	 * @param pwd Password for spark cluster
	 * @throws MalformedURLException
	 */
	public LivyBatchClient(String uri, String user, String pwd) throws MalformedURLException {
		baseUri = new URL(uri);
		auth = new BasicAuthenticator(user, pwd);
	}
	
	/**
	 * Gets all active interactive sessions
	 * @return All active sessions in JSON format.
	 * @throws IOException
	 */
	public String getActiveSessions() throws IOException {
		BufferedReader br = null;
		StringBuilder buf = new StringBuilder();
		
		HttpURLConnection con = (HttpURLConnection)new URL(baseUri + "/batches").openConnection();  
		Authenticator.setDefault(auth);
		con.setRequestMethod("GET");
		con.setRequestProperty("Content-Type", "application/json");
		br = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));  
		String line = null;
		try {
			while ((line = br.readLine()) != null) {
				buf.append(line);
				buf.append("\r\n");  
			}
		}finally{
			if(br != null)	br.close();
		}
	
		return buf.toString();
	}

	/**
	 * Creates a new batch job
 	 * @param req BatchJobParameters object
 	 * @return Session object that contains the session id returned from livy server
 	 * @throws IOException
 	 * @throws LivyException
 	 */
	public Session createJob(BatchJobParameters req) throws IOException, LivyException {

		String data = JsonConverter.toJson(req);
		DataOutputStream os = null;
		BufferedReader br = null;
		int status = Session.RUNNING;

		HttpURLConnection con = (HttpURLConnection)new URL(baseUri +  "/batches").openConnection();  
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

        	session =  new BatchSession(Integer.valueOf(root.get("id").asText()));
        	String state = root.get("state").asText();
        	
			if(state.equals("running") == true) status = Session.RUNNING;
        	else if(state.equals("error") == true) status = Session.ERROR;
        	else if(state.equals("success") == true) status = Session.SUCCESS;
        	else if(state.equals("starting") == true) status = Session.STARTING;
        	else if(state.equals("idle") == true) status = Session.IDLE;
        	else status = Session.ERROR;
        	session.setState(status);
        } finally {
        	if(br != null) br.close();
        	if(os != null) os.close();
        }

		return session;
	}

	/**
	 * Retrieves the current session information
	 * @return Session object
	 */
	public BatchSession getCurrentSession() {
		return session;
	}
	
	/**
	 * Gets the state of a batch session.
	 * @return Session's status
	 * @throws IOException
	 */
	public BatchSession getSession() throws IOException {
		int state = Session.STARTING;
		
		if(session.getState() == Session.DEAD) return session;
		
		HttpURLConnection con = (HttpURLConnection)new URL(baseUri + "/batches/" + session.getId()).openConnection();  
		Authenticator.setDefault(auth);
		con.setRequestMethod("GET");
		con.setRequestProperty("Content-Type", "application/json");
		          
	    StringBuilder sb = new StringBuilder();
	    String line = null;
	    try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"))) {
	    	while ((line = br.readLine()) != null) {
	    		sb.append(line);
	    		sb.append("\r\n");
	    	}
	    } catch(FileNotFoundException ex) {
	    	throw new IOException(ex);
	    }

	    String buf = sb.toString();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(buf);
		String st = root.get("state").asText();

    	if(st != null) {
    		if(st.equals("running") == true) state = Session.RUNNING;
    		else if(st.equals("error") == true) state = Session.ERROR;
    		else if(st.equals("success") == true) state = Session.SUCCESS;
    		else if(st.equals("starting") == true) state = Session.STARTING;
    		else if(st.equals("idle") == true) state = Session.IDLE;
    		else state = Session.ERROR;
    		session.setState(state);
    	}else{
    		session.setState(Session.ERROR);
    	}
    	if(root.get("appId") != null) session.setAppId(root.get("appId").asText());
    	if(root.get("appInfo") != null) session.setAppInfo(root.get("appInfo").asText());
    	if(root.get("log") != null) session.setLog(root.get("log").asText());
	
		return session;
	}
	
	/**
	 * Obtains full logs for the given batch id.
	 * @return log data
	 * @throws IOException
	 */
	public String getFullLog() throws IOException {
		BufferedReader br = null;
		
		if(session.getState() == Session.DEAD) return null;
		
		HttpURLConnection con = (HttpURLConnection)new URL(baseUri + "/batches/" + session.getId() + "/log").openConnection();  
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
	    String from = root.get("from").asText();
	    String total = root.get("total").asText();
    	String log = root.get("log").asText();
	
		return buf;
	}
	
	/**
	 * Deletes the interactive session
	 * @throws IOException
	 */
	public void deleteSession() throws IOException {
		BufferedReader br = null;
		
		try {
			HttpURLConnection con = (HttpURLConnection)new URL(baseUri + "/batches/" + session.getId()).openConnection();  
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
	    	session.reset();
	    }
	}
}
