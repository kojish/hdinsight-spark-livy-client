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
package sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import kojish.spark.livy.client.*;

public class InteractiveSample2 {

	private int session_status = Session.STARTING;
	private LivyInteractiveClient client = null;
	private final String AZUREHDINSIGHT_LIVY_URI = ".azurehdinsight.net/livy";
	//private String endpoint = "your-endpoint-name";
	
	public InteractiveSample2(String username, String password, String endpoint) {
		String baseUri = "https://" + endpoint + AZUREHDINSIGHT_LIVY_URI;
		try {
			client = new LivyInteractiveClient(baseUri, username, password);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	// Add escape sequence for double quote character in the statement.
	private String checkStatement(String statement) {
		StringBuilder sb = new StringBuilder();
		int start = 0, end;
		String buf = statement;
		
		while(buf.length() > 0) {
			end = buf.indexOf("\"");
			if(end < 0) {
				sb.append(buf);
				break;
			}
			sb.append(buf.substring(start, end));
			sb.append("\\");
			sb.append("\"");
			buf = buf.substring(end + 1);
			start = 0;
		}

		return sb.toString();
	}	
	
	public void run() {
		InteractiveSessionConf sc = new InteractiveSessionConf(SessionKind.SPARK);
//		String[] path = new String[1];
//		path[0] = "wasb://<blob-container-name>@<accountname>.blob.core.windows.net/jars/your.jar";
//		sc.setJars(path);
		
		// Set session listener
		try {
			client.createSession(sc);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (LivyException e) {
			e.printStackTrace();
		}
		
		while(true) {
			try {
				session_status = client.getSession().getState();
				if(session_status == Session.IDLE) {
					break;
				} else {
					System.out.println("Session is starting. Session ID: " + client.getSession().getId());
				}
				Thread.sleep(1000);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Ready for your input ....");
		System.out.print(">");
	
		while(true) {
			String input = null;
			// Ready to submit a statement
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			try{
				input = br.readLine();
			}catch(IOException e){
				System.out.println("Input Failure: " + e.getMessage());
			}
			String statement = checkStatement(input);
			
			System.out.println("Your input is [" + statement + "]");
			if(statement.equals("exit")) {
				try {
					client.deleteSession();
				System.out.println("Bye. The session is closed.");
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
			}

			try {
				client.submitStatement(statement, 1000, new StatementResultListener() {
					@Override
					public void update(StatementResult result) {
						System.out.println("Update Received. " + result.getOutput());
						System.out.println("Ready for your input ....");
						System.out.print(">");
					}
				});
			} catch (LivyException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
