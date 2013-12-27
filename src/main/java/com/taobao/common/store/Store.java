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
package com.taobao.common.store;

import java.io.IOException;
import java.util.Iterator;

/**
 * <b>存储的接口</b>
 * <p>简单的存储，对byte[]类型的key value对支持，
 * 仅支持add remove update三种数据操作</p>
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
public interface Store {
	/**
	 * 添加一个数据
	 * @param key
	 * @param data
	 * @throws IOException
	 */
	void add(byte[] key, byte[] data) throws IOException;
	
	/**
	 * 删除一个数据
	 * @param key
	 * @return 是否删除了数据
	 * @throws IOException
	 */
	boolean remove(byte[] key) throws IOException;
	
	/**
	 * 获取一个数据
	 * @param key
	 * @return 获得的数据，如果没有，返回null
	 * @throws IOException
	 */
	byte[] get(byte[] key) throws IOException;
	
	/**
	 * 更新一个数据
	 * @param key
	 * @param data
	 * @return 是否有更新到
	 * @throws IOException
	 */
	boolean update(byte[] key, byte[] data) throws IOException;
	
	/**
	 * 获取数据个数
	 * @return 数据个数
	 * @throws IOException
	 */
	int size() throws IOException;
	
	/**
	 * 遍历key
	 * @return key的遍历器
	 * @throws IOException
	 */
	Iterator<byte[]> iterator() throws IOException;
	
	/**
	 * 关闭存储
	 * @throws IOException
	 */
	void close() throws IOException;
}
