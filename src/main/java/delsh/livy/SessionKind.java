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
 * The enumeration types for session kind.
 */
public enum SessionKind {
	// Possible values are the following.
	SPARK("spark"),
	PYSPARK("pyspark"),
	SPARKR("sparkr");
	
	private String kind;
	
	private SessionKind(String skind) {
		kind = skind;
	}

	public String toString() {
		return kind;
	}

	/**
	 * This class finds enum value that is equivalent to a given string.
	 *	@param kind Session kind.
	 *	@return Enum value
	 */
	public static SessionKind getEnum(String str) {
		SessionKind[] array = SessionKind.values();
		for(SessionKind enumStr : array) {
			if(str.equals(enumStr.kind.toString())) {
				return enumStr;
			}
		}
		return null;
	}
}
