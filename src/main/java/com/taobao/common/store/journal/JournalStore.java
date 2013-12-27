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
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.taobao.common.store.Store;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.Util;

/**
 * <b>一个通过日志文件实现的key/value对的存储</b>
 * 
 * key必须是16字节 <br />
 * 1、数据文件和日志文件在一起，不记录索引文件<br />
 * 	 name.1 name.1.log<br />
 * 2、data为真正的数据，顺序存放，使用引用计数<br />
 * 3、log为操作+key+偏移量<br />
 * 4、添加数据时，先添加name.1，获得offset和length，然后记录日志，增加引用计数，然后加入或更新内存索引<br />
 * 5、删除数据时，记录日志，删除内存索引，减少文件计数，判断大小是否满足大小了，并且无引用了，就删除数据文件和日志文件<br />
 * 6、获取数据时，直接从内存索引获得数据偏移量<br />
 * 7、更新数据时，调用添加<br />
 * 8、启动时，遍历每一个log文件，通过日志的操作恢复内存索引<br />
 * 
 * @author dogun (yuexuqiang at gmail.com)
 */
public class JournalStore implements Store, JournalStoreMBean {
	static Logger log = Logger.getLogger(JournalStore.class);
	
	public static final int FILE_SIZE = 1024 * 1024 * 50; //20M
	
	private String path;
	private String name;
	private boolean force;
	
	private Map<BytesKey, OpItem> indices = new ConcurrentHashMap<BytesKey, OpItem>(10000, 0.8F, 40);
	private Map<Integer, DataFile> dataFiles = new ConcurrentHashMap<Integer, DataFile>();
	private Map<Integer, LogFile> logFiles = new ConcurrentHashMap<Integer, LogFile>();
	
	private DataFile dataFile = null;
	private LogFile logFile = null;
	private AtomicInteger number = new AtomicInteger(0);
	
	private ReentrantLock addLock = new ReentrantLock();
	
	/**
	 * 默认构造函数，会在path下使用name作为名字生成数据文件
	 * @param path
	 * @param name
	 * @param force
	 * @throws IOException
	 */
	public JournalStore(String path, String name, boolean force) throws IOException {
		Util.registMBean(this, name);
		this.path = path;
		this.name = name;
		this.force = force;
		
		addLock.lock();
		try {
			initLoad();
			//如果当前没有可用文件，生成
			if (null == this.dataFile || null == this.logFile) {
				newDataFile();
			}
			//准备好了
		} finally {
			addLock.unlock();
		}
		
		//当应用被关闭的时候,如果没有关闭文件,关闭之.对某些操作系统有用
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					close();
				} catch (IOException e) {
					log.error("close error", e);
				}
			}
		});
	}	
	
	/**
	 * 默认构造函数，会在path下使用name作为名字生成数据文件
	 * @param path
	 * @param name
	 * @throws IOException
	 */
	public JournalStore(String path, String name) throws IOException {
		this(path, name, false);
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#add(byte[], byte[])
	 */
	public void add(byte[] key, byte[] data) throws IOException {
		//先检查是否已经存在，如果已经存在抛出异常 判断文件是否满了，添加name.1，获得offset，记录日志，增加引用计数，加入或更新内存索引
		checkParam(key, data);
		addLock.lock();
		try {
			innerAdd(key, data);
		} finally {
			addLock.unlock();
		}
	}

	/**
	 * 内部添加数据
	 * @param key
	 * @param data
	 * @throws IOException
	 */
	private OpItem innerAdd(byte[] key, byte[] data)
			throws IOException {
		BytesKey k = new BytesKey(key);
		if (this.indices.containsKey(k)) {
			throw new IOException("发现重复的key");
		}
		if (this.dataFile.getLength() >= FILE_SIZE) { //满了
			newDataFile();
		}
		
		int num = this.number.get();
		DataFile df = this.dataFile;
		LogFile lf = this.logFile;
		
		if (null != df && null != lf) {
			long pos = df.write(ByteBuffer.wrap(data));
			OpItem op = new OpItem();
			op.key = key;
			op.length = data.length;
			op.offset = pos - op.length;
			op.op = OpItem.OP_ADD;
			op.number = num;
			lf.write(ByteBuffer.wrap(op.toByte()));
			df.increment();
			this.indices.put(k, op);
			return op;
		} else {
			throw new IOException("文件在使用的同时被删除了:" + num);
		}
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#get(byte[])
	 */
	public byte[] get(byte[] key) throws IOException {
		OpItem op = this.indices.get(new BytesKey(key));
		byte[] data = null;
		if (null != op) {
			DataFile df = this.dataFiles.get(new Integer(op.number));
			if (null != df) {
				ByteBuffer bf = ByteBuffer.wrap(new byte[(int)op.length]);
				df.read(bf, op.offset);
				data = bf.array();
			} else {
				log.warn("数据文件丢失：" + op);
			}
		}
		return data;
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#iterator()
	 */
	public Iterator<byte[]> iterator() throws IOException {
		final Iterator<BytesKey> it = this.indices.keySet().iterator();
		return new Iterator<byte[]>() {
			public boolean hasNext() {
				return it.hasNext();
			}
			public byte[] next() {
				BytesKey bk = it.next();
				if (null != bk) {
					return bk.getData();
				}
				return null;
			}
			public void remove() {
				throw new UnsupportedOperationException("不支持删除，请直接调用store.remove方法");
			}
		};
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#remove(byte[])
	 */
	public boolean remove(byte[] key) throws IOException {
		//获得记录在那个文件，记录日志，删除内存索引，减少文件计数，判断大小是否满足大小了，并且无引用了，就删除数据文件和日志文件
		boolean ret = false;
		addLock.lock();
		try {
			ret = innerRemove(key);
		} finally {
			addLock.unlock();
		}
		return ret;
	}

	/**
	 * 内部删除
	 * @param key
	 * @return 是否删除了数据
	 * @throws IOException
	 */
	private boolean innerRemove(byte[] key) throws IOException {
		boolean ret = false;
		BytesKey k = new BytesKey(key);
		OpItem op = this.indices.get(k);
		if (null != op) {
			ret = innerRemove(op);
			if(ret){
				this.indices.remove(k);
			}
		}
		return ret;
	}

	/**
	 * 根据OpItem对象，在日志文件中记录删除的操作日志，并且修改对应数据文件的引用计数.
	 * @param op
	 * @return
	 * @throws IOException
	 */
	private boolean innerRemove(OpItem op) throws IOException {
		DataFile df = this.dataFiles.get(new Integer(op.number));
		LogFile lf = this.logFiles.get(new Integer(op.number));
		if(null != df && null != lf){
			OpItem o = new OpItem();
			o.key = op.key;
			o.length = op.length;
			o.number = op.number;
			o.offset = op.offset;
			o.op = OpItem.OP_DEL;
			lf.write(ByteBuffer.wrap(o.toByte()));
			df.decrement();
			//判断是否可以删了
			if (df.getLength() >= FILE_SIZE && df.isUnUsed()) {
				if (this.dataFile == df) { //判断如果是当前文件，生成新的
					newDataFile();
				}
				log.info("删除文件：" + df);
				this.dataFiles.remove(new Integer(op.number));
				this.logFiles.remove(new Integer(op.number));
				df.delete();
				lf.delete();
			}
			return true;
		}
		return false;
	}
	
	/**
	 * 检查参数是否合法
	 * @param key
	 * @param data
	 */
	private void checkParam(byte[] key, byte[] data) {
		if (null == key || null == data) throw new NullPointerException("key/data can't be null");
		if (key.length != 16) throw new IllegalArgumentException("key.length must be 16");
	}
	
	/**
	 * 生成一个新的数据文件
	 * @throws FileNotFoundException
	 */
	private void newDataFile()
			throws IOException {
		int n = this.number.incrementAndGet();
		this.dataFile = new DataFile(new File(path + File.separator + name + "." + n), force);
		this.logFile = new LogFile(new File(path + File.separator + name + "." + n + ".log"), force);
		this.dataFiles.put(new Integer(n), this.dataFile);
		this.logFiles.put(new Integer(n), this.logFile);
		log.info("生成新文件：" + this.dataFile);
	}

	/**
	 * 类初始化的时候，需要遍历所有的日志文件，恢复内存的索引
	 * @throws IOException
	 */
	private void initLoad() throws IOException {
		log.warn("开始恢复数据");
		final String nm = name + ".";
		File dir = new File(path);
		File[] fs = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String n) {
				return n.startsWith(nm) && !n.endsWith(".log");
			}
		});
		log.warn("遍历每个数据文件");
		List<Integer> indexList = new LinkedList<Integer>();
		for (File f : fs) {
			try{
				String fn = f.getName();
				int n = Integer.parseInt(fn.substring(nm.length()));
				indexList.add(new Integer(n));
			}
			catch(Exception e){
				log.error("parse file index error" + f, e);
			}
		}
		
		Integer[] indices = indexList.toArray(new Integer[indexList.size()]);
		
		//对文件顺序进行排序
		Arrays.sort(indices);
		
		for (Integer n : indices) {
			log.warn("处理index为" +n + "的文件");
			//保存本数据文件的索引信息
			Map<BytesKey, OpItem> idx = new HashMap<BytesKey, OpItem>();
			//生成dataFile和logFile
			File f = new File(dir, name + "." + n);
			DataFile df = new DataFile(f, force);
			LogFile lf = new LogFile(new File(f.getAbsolutePath() + ".log"), force);
			long size = lf.getLength() / OpItem.LENGTH;
			
			for (int i = 0; i < size; ++i) { //循环每一个操作
				ByteBuffer bf = ByteBuffer.wrap(new byte[OpItem.LENGTH]);
				lf.read(bf, i * OpItem.LENGTH);
				if (bf.hasRemaining()) {
					log.warn("log file error:" + lf + ", index:" + i);
					continue;
				}
				OpItem op = new OpItem();
				op.parse(bf.array());
				BytesKey key = new BytesKey(op.key);
				switch(op.op){
				case OpItem.OP_ADD: //如果是添加的操作，加入索引，增加引用计数
					OpItem o = this.indices.get(key);
					if(null != o){
						//已经在之前添加过，那么必然是Update的时候，Remove的操作日志没有写入。
						
						//写入Remove日志
						innerRemove(o);
						
						//从map中删除
						this.indices.remove(key);
					}
					boolean addRefCount = true;
					if(idx.get(key) != null){
						//在同一个文件中add或者update过，那么只是更新内容，而不增加引用计数。
						addRefCount = false;
					}
					
					idx.put(key, op);

					if(addRefCount){
						df.increment();
					}
					break;

				case OpItem.OP_DEL: //如果是删除的操作，索引去除，减少引用计数
					idx.remove(key);
					df.decrement();
					break;
										
				default :
					log.warn("unknow op:" + (int)op.op);
					break;
				}
			}
			if (df.getLength() >= FILE_SIZE && df.isUnUsed()) { //如果这个数据文件已经达到指定大小，并且不再使用，删除
				df.delete();
				lf.delete();
				log.warn("不用了，也超过了大小，删除");
			} else { //否则加入map
				this.dataFiles.put(n, df);
				this.logFiles.put(n, lf);
				if (!df.isUnUsed()) { //如果有索引，加入总索引 
					this.indices.putAll(idx);
					log.warn("还在使用，放入索引，referenceCount:" + df.getReferenceCount() + ", index:" + idx.size());
				}
			}
		}
		//校验加载的文件，并设置当前文件
		if(this.dataFiles.size() > 0){
			indices = this.dataFiles.keySet().toArray(new Integer[0]);
			Arrays.sort(indices);
			for(int i=0; i < indices.length - 1; i++){
				DataFile df = this.dataFiles.get(indices[i]);
				if(df.isUnUsed() || df.getLength() < FILE_SIZE){
					throw new IllegalStateException("非当前文件的状态是大于等于文件块长度，并且是used状态");
				}
			}
			Integer n = indices[indices.length - 1];
			this.number.set(n.intValue());
			this.dataFile = this.dataFiles.get(n);
			this.logFile = this.logFiles.get(n);
		}
		log.warn("恢复数据：" + this.size());
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#size()
	 */
	public int size() throws IOException {
		return this.indices.size();
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#update(byte[], byte[])
	 */
	public boolean update(byte[] key, byte[] data) throws IOException {
		addLock.lock();
		try {
			//对于Update的消息，我们写入OpCode为Update的日志。
			BytesKey k = new BytesKey(key);
			OpItem op = this.indices.get(k);
			if(null != op){
				this.indices.remove(k);
				OpItem o = innerAdd(key, data);
				if(o.number != op.number){
					//不在同一个文件上更新，才进行删除。
					innerRemove(op);
				}
				else{
					DataFile df = this.dataFiles.get(new Integer(op.number));
					df.decrement();
				}
				return true;
			}
		} finally {
			addLock.unlock();
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getDataFilesInfo()
	 */
	public String getDataFilesInfo() {
		return this.dataFiles.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getLogFilesInfo()
	 */
	public String getLogFilesInfo() {
		return this.logFiles.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getNumber()
	 */
	public int getNumber() {
		return this.number.get();
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getPath()
	 */
	public String getPath() {
		return path;
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getName()
	 */
	public String getName() {
		return name;
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getDataFileInfo()
	 */
	public String getDataFileInfo() {
		return this.dataFile.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getLogFileInfo()
	 */
	public String getLogFileInfo() {
		return this.logFile.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#viewIndexMap()
	 */
	public String viewIndexMap() {
		return indices.toString();
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.Store#close()
	 */
	public void close() throws IOException {
		for (DataFile df : this.dataFiles.values()) {
			try {
				df.close();
			} catch (Exception e) {
				log.warn("close error:" + df, e);
			}
		}
		this.dataFiles.clear();
		for (LogFile lf : this.logFiles.values()) {
			try {
				lf.close();
			} catch (Exception e) {
				log.warn("close error:" + lf, e);
			}
		}
		this.logFiles.clear();
		this.dataFile = null;
		this.logFile = null;
	}

	/* (non-Javadoc)
	 * @see com.taobao.common.store.journal.JournalStoreMBean#getSize()
	 */
	public long getSize() throws IOException {
		return size();
	}
}
