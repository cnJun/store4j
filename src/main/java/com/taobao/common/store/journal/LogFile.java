/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package com.taobao.common.store.journal;

import java.io.File;
import java.io.IOException;


/**
 * 一个日志文件
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
class LogFile extends DataFile {
	/**
	 * 默认构造函数
	 * @param file
	 * @throws IOException
	 */
	LogFile(File file) throws IOException {
		this(file, false);
	}
	
	/**
	 * 构造函数
	 * @param file
	 * @param force
	 * @throws IOException
	 */
	LogFile(File file, boolean force) throws IOException {
		super(file, force);
		//这个地方是为了防止操作日志文件的不完整。如果不完整，则丢弃最后不完整的数据。
		long count = fc.size() / OpItem.LENGTH;
		if(count * OpItem.LENGTH < fc.size()){
			fc.truncate(count * OpItem.LENGTH);
			fc.position(count * OpItem.LENGTH);
		}
	}
	
}
