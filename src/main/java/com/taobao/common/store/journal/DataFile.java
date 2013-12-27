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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 代表了一个数据文件
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
class DataFile {
	private File file;
	private AtomicInteger referenceCount = new AtomicInteger(0);
	protected FileChannel fc;
	protected RandomAccessFile raf;

	/**
	 * 构造函数，会打开指定的文件，并且将指针指向文件结尾
	 * @param file
	 * @throws IOException
	 */
	DataFile(File file) throws IOException {
		this(file, false);
	}
	
	
	/**
	 * 构造函数，会打开指定的文件，并且将指针指向文件结尾
	 * @param file
	 * @throws IOException
	 */
	DataFile(File file, boolean force) throws IOException {
		this.file = file;
		raf = new RandomAccessFile(file, force?"rws":"rw");
		fc = raf.getChannel();
		//指针移到最后
		fc.position(fc.size());
	}

	/**
	 * 获得文件的大小
	 * 
	 * @return 文件的大小
	 * @throws IOException
	 */
	long getLength() throws IOException {
		return fc.size();
	}

	/**
	 * 删除文件
	 * @return 是否删除成功
	 * @throws IOException
	 */
	boolean delete() throws IOException {
		close();
		return file.delete();
	}

	/**
	 * 强制将数据写回硬盘
	 * 
	 * @throws IOException
	 */
	void force() throws IOException {
		fc.force(true);
	}

	/**
	 * 关闭文件
	 * 
	 * @throws IOException
	 */
	void close() throws IOException {
		fc.close();
		raf.close();
	}
	
	/**
	 * 从文件读取数据到bf，直到读满或者读到文件结尾。
	 * <br />
	 * 文件的指针会向后移动bf的大小
	 * 
	 * @param bf
	 * @throws IOException
	 */
	void read(ByteBuffer bf) throws IOException {
		while (bf.hasRemaining()) {
			int l = fc.read(bf);
			if (l < 0) break;
		}
	}

	/**
	 * 从文件的制定位置读取数据到bf，直到读满或者读到文件结尾。
	 * <br />
	 * 文件指针不会移动
	 * 
	 * @param bf
	 * @param offset
	 * @throws IOException
	 */
	void read(ByteBuffer bf, long offset) throws IOException {
		int size = 0;
		while (bf.hasRemaining()) {
			int l = fc.read(bf, offset + size);
			size += l;
			if (l < 0) break;
		}
	}

	/**
	 * 写入bf长度的数据到文件，文件指针会向后移动
	 * @param bf
	 * @return 写入后的文件position
	 * @throws IOException
	 */
	long write(ByteBuffer bf) throws IOException {
		while (bf.hasRemaining()) {
			int l = fc.write(bf);
			if (l < 0) break;
		}
		return fc.position();
	}
	
	/**
	 * 从指定位置写入bf长度的数据到文件，文件指针<b>不会</b>向后移动
	 * @param offset
	 * @param bf
	 * @throws IOException
	 */
	void write(long offset, ByteBuffer bf) throws IOException {
		int size = 0;
		while (bf.hasRemaining()) {
			int l = fc.write(bf, offset + size);
			size += l;
			if (l < 0) break;
		}
	}

	/**
	 * 对文件增加一个引用计数
	 * @return 增加后的引用计数
	 */
	int increment() {
		return referenceCount.incrementAndGet();
	}

	/**
	 * 对文件减少一个引用计数
	 * @return 减少后的引用计数
	 */
	int decrement() {
		return referenceCount.decrementAndGet();
	}

	/**
	 * 文件是否还在使用（引用计数是否是0了）
	 * @return 文件是否还在使用
	 */
	boolean isUnUsed() {
		return getReferenceCount() <= 0;
	}
	
	/**
	 * 获得引用计数的值
	 * @return 引用计数的值
	 */
	int getReferenceCount() {
		return this.referenceCount.get();
	}

	@Override
	public String toString() {
		String result = null;
		try {
			result = file.getName() + " , length = " + getLength()
					+ " refCount = " + referenceCount + " position:" + fc.position();
		} catch (IOException e) {
			result = e.getMessage();
		}
		return result;
	}
}
