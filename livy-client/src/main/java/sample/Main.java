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
import java.io.PrintStream;
import java.util.HashMap;

public class Main {

	// When you create an interactive session using InteractiveSample class,
	// it will try to create the session that takes about 15 - 30 sec.
	// Once the session creation is completed, it is ready for taking your input from stdin.
	//
	// For example, you can put the following:
	// >val sqlContext = new org.apache.spark.sql.SQLContext(sc); val pf = sqlContext.read.parquet("wasb:///example/data/people.parquet"); pf.registerTempTable("pftbl"); sqlContext.sql("SELECT * FROM pftbl").show()
	//
	// Then, the query result will be returned as below:
	// +---+-----+
	// |age| name|
	// +---+-----+
	// | 22|Ricky|
	// | 36| Jeff|
	// | 62|Geddy|
	// +---+-----+
	// 

	public static void main(String args[]) throws Exception {
    
        HashMap<String, String> opts = Main.getOpt(args);
         
        if ( opts.isEmpty() || opts.size() != 4) {
            System.err.println("The number of parameter is wrong. Please see the below.");
            printUsage(System.out);
            System.exit(1);
        }    
        else if (opts.get("mode").equals("interactive")) {
		    InteractiveSample2 client = new InteractiveSample2(opts.get("username"), opts.get("password"), opts.get("endpoint"));
		    client.run();
        }    
        else if (opts.get("mode").equals("batch")) {
		    BatchSample client = new BatchSample(opts.get("username"), opts.get("password"), opts.get("endpoint"));
		    client.run();
        }
        else if (opts.containsKey("mode") {
            System.err.println("There is no mode you select. Please see the below.");
            printUsage(System.out);
            System.exit(1);
        }
        else {
            System.err.println("Any parameter is wrong. Please see the below.");
            printUsage(System.out);
            System.exit(1);
        }
    }
    
    public static HashMap<String, String> getOpt(String args[]) {

        HashMap<String, String> opts = new HashMap<String, String>();

        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-u")) {
                opts.put("username", args[i + 1]);
            }
            if (args[i].startsWith("-p")) {
                opts.put("password", args[i + 1]);
            }
            if (args[i].startsWith("-e")) {
                opts.put("endpoint", args[i + 1]);
            }
            if (args[i].startsWith("-m")) {
                opts.put("mode", args[i + 1]);
            }
            if (args[i].startsWith("-h")) {
                printUsage(System.out);
                System.exit(0);
            }
        }

        return opts;

    }
    
    public static void printUsage(PrintStream s) {
        s.println("Usage: \"java -cp \"path-to-json-simple-jar\"/json-simple-1.1.1.jar:\"path-to-target-directory\"/livy-client-0.0.1-SNAPSHOT.jar sample/Main [options]");
        s.println("Options:");
        s.println("\t-u                Specifies the username to log in HDInsight cluster.");
        s.println("\t-p                Specifies the password to log in HDInsight cluster.");
        s.println("\t-e                Specifies the endpoint of HDInsight cluster.");
        s.println("\t-m                Selects either \"interactive\" or \"batch\" for the execution mode.");
        s.println("\t-h                Prints this message and quit.");
    }

}
