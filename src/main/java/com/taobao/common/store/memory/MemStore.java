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
package com.taobao.common.store.memory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.common.store.Store;
import com.taobao.common.store.util.BytesKey;

/**
 * 一个使用内存保存key/value对的实现，性能好，但是内存占用比较多
 * 
 * @author dogun (yuexuqiang at gmail.com)
 *
 */
public class MemStore implements Store {
	private Map<BytesKey, byte[]> datas = new ConcurrentHashMap<BytesKey, byte[]>();
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#add(byte[], byte[])
	 */
	public void add(byte[] key, byte[] data) throws IOException {
		datas.put(new BytesKey(key), data);
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#get(byte[])
	 */
	public byte[] get(byte[] key) throws IOException {
		return datas.get(new BytesKey(key));
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#iterator()
	 */
	public Iterator<byte[]> iterator() throws IOException {
		final Iterator<BytesKey> it = datas.keySet().iterator();
		return new Iterator<byte[]>() {
			public boolean hasNext() {
				return it.hasNext();
			}

			public byte[] next() {
				BytesKey key = it.next();
				if (null == key) {
					return null;
				}
				return key.getData();
			}

			public void remove() {
				it.remove();
			}
		};
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#remove(byte[])
	 */
	public boolean remove(byte[] key) throws IOException {
		return null != datas.remove(new BytesKey(key));
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#size()
	 */
	public int size() throws IOException {
		return datas.size();
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#update(byte[], byte[])
	 */
	public boolean update(byte[] key, byte[] data) throws IOException {
		datas.put(new BytesKey(key), data);
		return true;
	}

	public void close() throws IOException {
		//nodo
	}
}
