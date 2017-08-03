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

import java.io.IOException;
import java.net.MalformedURLException;
import ms.csu.livy.client.*;

public class BatchSample {

	private LivyBatchClient client = null;
	private final String AZUREHDINSIGHT_LIVY_URI = ".azurehdinsight.net/livy";
	private String endpoint = "your-endpoint-name";
	
	public BatchSample() {
		String baseUri = "https://" + endpoint + AZUREHDINSIGHT_LIVY_URI;
		try {
			client = new LivyBatchClient(baseUri, "username", "password");
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		BatchSessionConf sc = new BatchSessionConf("wasb:///jars/sample.jar", "SparkSqlSample");
//		String[] path = new String[1];
//		path[0] = "wasb://<blob-container-name>@<accountname>.blob.core.windows.net/jars/your.jar";
//		sc.setJars(path);
	
		try {
			//System.out.println(client.getActiveSessions());
			client.createJob(sc);
			int status = Session.NOT_STARTED;
			while(true) {
				status = client.getSession().getState();
		    	if(status == Session.RUNNING) {
		    		System.out.println("Status: RUNNING");
					System.out.println(" AppId: " + client.getSession().getAppId());
					System.out.println(" AppInfo: " + client.getSession().getAppInfo());
					System.out.println(" Id: " + client.getSession().getId());
					System.out.println(" Log: " + client.getSession().getLog());
		    	}
		    	else if(status == Session.SUCCESS) {
		    		System.out.println("Status: SUCCESS");
		    		break;
		    	}
		    	else if(status == Session.ERROR) {
		    		System.out.println("Status: ERROR");
		    		break;
		    	}
		    	else {
		    		System.out.println("Status: None");
		    		break;
		    	}
				Thread.sleep(1000);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (LivyException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
