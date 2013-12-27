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

import java.nio.ByteBuffer;


/**
 * 一个日志记录 操作+数据key+数据文件编号+偏移量+长度
 * 
 * @author dogun (yuexuqiang at gmail.com)
 *
 */
public class OpItem {
	public static final byte OP_ADD = 1;
	public static final byte OP_DEL = 2;
	
	public static final int KEY_LENGTH = 16;
	public static final int LENGTH = KEY_LENGTH + 1 + 4 + 8 + 4;
	
	byte op;
	byte[] key;
	int number;
	long offset;
	int length;
	
	/**
	 * 将一个操作转换成字节数组
	 * 
	 * @return 字节数组
	 */
	byte[] toByte() {
		byte[] data = new byte[LENGTH];
		ByteBuffer bf = ByteBuffer.wrap(data);
		bf.put(key);
		bf.put(op);
		bf.putInt(number);
		bf.putLong(offset);
		bf.putInt(length);
		return bf.array();
	}

	/**
	 * 通过字节数组构造成一个操作日志
	 * @param data
	 */
	void parse(byte[] data) {
		ByteBuffer bf = ByteBuffer.wrap(data);
		key = new byte[16];
		bf.get(key);
		op = bf.get();
		number = bf.getInt();
		offset = bf.getLong();
		length = bf.getInt();
	}
	
	@Override
	public String toString() {
		return "OpItem number:" + number + ", op:" + (int)op + ", offset:" + offset + ", length:" + length;
	}
}
