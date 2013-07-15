/**
 * Copyright 1999-2011 Alibaba Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.alibaba.cobar.client.audit;

/**
 * group by the SQL statement when auditing them.<br>
 * 
 * the group-by SQL will be published to audit server or be fetched by audit server in a periodic way.<br>
 * 
 * simple handling is OK, but you can escalate to a more scalable solution like store SQL statement into NoSQL storage 
 * and do analysis later with powerful computing cluster.  

 * @author fujohnwang
 * @since  1.0 
 */
public interface ISqlAuditor {
	void audit(String id, String sql, Object sqlContext);
}
