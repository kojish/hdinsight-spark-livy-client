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

public class Main {

	// When you create an interactive session using InteractiveSample class,
	// it will try to create the session that takes about 15 - 30 sec.
	// Once the session creation is completed, it is ready for taking your
	// input from the console.
	//
	// For example,you can put the following:
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
		InteractiveSample1 client = new InteractiveSample1();
		//BatchSample client = new BatchSample();
		client.run();
   }
}
