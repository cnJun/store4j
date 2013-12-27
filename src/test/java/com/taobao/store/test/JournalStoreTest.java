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
package com.taobao.store.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.store.journal.JournalStore;
import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.UniqId;

/**
 * @author dogun (yuexuqiang at gmail.com)
 * @author lin wang(xalinx at gmail dot com)
 * @date 2007-12-10
 */
public class JournalStoreTest {
    JournalStore store = null;

    private String getPath(){
    	return "tmp" + File.separator + "notify-store-test";
    }
    
    private String getStoreName(){
    	return "testStore";
    }
    
    @Before
    public void setUp() throws Exception {
        String path = getPath();
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("can't make dir " + dir);
        }

        File[] fs = dir.listFiles();
        for (File f : fs) {
            if (!f.delete()) {
                throw new IllegalStateException("can't delete " + f);
            }
        }

        this.store = new JournalStore(path, getStoreName());
        assertEquals(0, this.store.size());
    }

    @After
    public void after() throws IOException {
        if (store != null) {
            store.close();
        }
    }
    
    /**
     * 测试某个文件不是最后一个文件，文件不被使用，文件的大小超过了FILE_SIZE，在创建Store的时候会被删除。
     * @throws Exception
     */
    @Test
    public void testDeletingUnusedNonCurrentFileWhileCreatingStore() throws Exception {
    	after();
    	String filePrefix = getFilePrefix();
    	//创建测试环境。写满一个文件，然后在第二个文件中插入一个信息，然后做一个Broken的Update，也就是加入了新的，但是没有remove
    	RandomAccessFile df = new RandomAccessFile(filePrefix + "1", "rw");
    	RandomAccessFile lf = new RandomAccessFile(filePrefix + "1.log", "rw");
    	final int messageLength = 1024*1024 * 8;
    	while(df.length() < JournalStore.FILE_SIZE){
    		byte[] key = UniqId.getInstance().getUniqIDHash();
    		long offset = add(df, lf, key, new byte[messageLength], 1);
    		remove(lf, key, offset, 1, messageLength);
    	}
    	df.close();
    	lf.close();    

    	df = new RandomAccessFile(filePrefix + "2", "rw");
    	lf = new RandomAccessFile(filePrefix + "2.log", "rw");
    	
    	df.close();
    	lf.close();
    	store = new JournalStore(getPath(), getStoreName());
    	
    	Assert.assertFalse(new File(filePrefix + "1").exists());
    	Assert.assertFalse(new File(filePrefix + "1.log").exists());
    	
    }

	private String getFilePrefix() {
		return getPath() + File.separator + getStoreName() + ".";
	}
    
    /**
     * 测试在同一个文件中添加和更新数据，更新的次数大于1次。要保证引用计数的正确。
     * 		添加一个message，然后做两次update，之后get出来的是最后一次update的结果。
     * 		然后删除之前添加的消息.
     * 		然后添加新的消息并删除消息，把这个文件填满.
     * 		然后验证文件是被删除了的。这个验证update对于引用计数的正确修改。
     * 
     * @throws Exception
     */
    @Test
    public void testAdd_UpdateInSameFile1() throws Exception {
    	byte[] key = UniqId.getInstance().getUniqIDHash();
    	store.add(key, "OriginalData".getBytes());
    	store.update(key, "FirstUpdate".getBytes());
    	store.update(key, "LastUpdate".getBytes());
    	
    	assertEquals(0, "LastUpdate".compareTo(new String(store.get(key))));
    	
    	final int messageLength = 1024 * 1024 * 8;
    	store.remove(key);
    	
    	final int count = JournalStore.FILE_SIZE / messageLength + 3;
    	
    	for(int i = 0; i < count; i++){
    		byte[] data = new byte[messageLength];
    		byte[] k = UniqId.getInstance().getUniqIDHash();
    		store.add(k, data);
    		store.remove(k);
    	}    	
    	
    	String filePrefix = getFilePrefix();
    	Assert.assertFalse(new File(filePrefix + "1").exists());
    	Assert.assertFalse(new File(filePrefix + "1.log").exists());
    }
    
    /**
     * 测试在同一个文件中添加和更新数据，更新的次数大于1次。要保证引用计数的正确。
     * 		添加一个message，然后做两次update
     * 		然后关闭store，重新创建store。然后get前面添加的消息，验证是最后Update的内容。
     *      然后添加新的消息并删除消息，把这个文件填满，然后删除之前添加的消息。
     * 		然后验证文件是被删除了的。
     * 
     * @throws Exception
     */
    @Test
    public void testAdd_UpdateInSameFile2() throws Exception {
    	byte[] key = UniqId.getInstance().getUniqIDHash();
    	store.add(key, "OriginalData".getBytes());
    	store.update(key, "FirstUpdate".getBytes());
    	store.update(key, "LastUpdate".getBytes());
    	
    	after();
    	
    	store = new JournalStore(getPath(), getStoreName());
    	assertEquals(0, "LastUpdate".compareTo(new String(store.get(key)))); 	
    	
    	final int messageLength = 1024 * 1024 * 8;
    	store.remove(key);
    	
    	final int count = JournalStore.FILE_SIZE / messageLength + 3;
    	
    	for(int i = 0; i < count; i++){
    		byte[] data = new byte[messageLength];
    		byte[] k = UniqId.getInstance().getUniqIDHash();
    		store.add(k, data);
    		store.remove(k);
    	}    	
    	
    	String filePrefix = getFilePrefix();
    	Assert.assertFalse(new File(filePrefix + "1").exists());
    	Assert.assertFalse(new File(filePrefix + "1.log").exists());
    }
    
    /**
     * 测试在同一个文件中添加和更新数据，更新的次数为1次。要保证引用计数的正确。
     * 		添加一个message，然后做1次update
     * 		然后删除之前添加的消息.
     * 		然后添加新的消息并删除消息，把这个文件填满.
     * 		然后验证文件是被删除了的。这个验证update对于引用计数的正确修改。
     * 
     * @throws Exception
     */
    @Test
    public void testAdd_UpdateInSameFile3() throws Exception {
    	byte[] key = UniqId.getInstance().getUniqIDHash();
    	store.add(key, "OriginalData".getBytes());
    	store.update(key, "LastUpdate".getBytes());
    	
    	assertEquals(0, "LastUpdate".compareTo(new String(store.get(key))));
    	
    	final int messageLength = 1024 * 1024 * 8;
    	store.remove(key);
    	
    	final int count = JournalStore.FILE_SIZE / messageLength + 3;
    	
    	for(int i = 0; i < count; i++){
    		byte[] data = new byte[messageLength];
    		byte[] k = UniqId.getInstance().getUniqIDHash();
    		store.add(k, data);
    		store.remove(k);
    	}    	
    	
    	String filePrefix = getFilePrefix();
    	Assert.assertFalse(new File(filePrefix + "1").exists());
    	Assert.assertFalse(new File(filePrefix + "1.log").exists());
    }
    
    /**
     * 测试在同一个文件中添加和更新数据，更新的次数为1次。要保证引用计数的正确。
     * 		添加一个message，然后做1次update
     * 		然后关闭store，重新创建store。然后get前面添加的消息，验证是最后Update的内容。
     *      然后添加新的消息并删除消息，把这个文件填满，然后删除之前添加的消息。
     * 		然后验证文件是被删除了的。
     * 
     * @throws Exception
     */
    @Test
    public void testAdd_UpdateInSameFile4() throws Exception {
    	byte[] key = UniqId.getInstance().getUniqIDHash();
    	store.add(key, "OriginalData".getBytes());
    	store.update(key, "LastUpdate".getBytes());
    	
    	after();
    	
    	store = new JournalStore(getPath(), getStoreName());
    	assertEquals(0, "LastUpdate".compareTo(new String(store.get(key)))); 	
    	
    	final int messageLength = 1024 * 1024 * 8;
    	store.remove(key);
    	
    	final int count = JournalStore.FILE_SIZE / messageLength + 3;
    	
    	for(int i = 0; i < count; i++){
    		byte[] data = new byte[messageLength];
    		byte[] k = UniqId.getInstance().getUniqIDHash();
    		store.add(k, data);
    		store.remove(k);
    	}    	
    	
    	String filePrefix = getFilePrefix();
    	Assert.assertFalse(new File(filePrefix + "1").exists());
    	Assert.assertFalse(new File(filePrefix + "1.log").exists());
    }    
 
    /**
     * 测试add和update不在同一个文件中的情况。update在同一个文件中
     * 		添加一个message，然后再添加另外一个message，然后填满这个文件。	
     * 		做1次update
     * 		检测第一个文件的日志的最后是一个删除记录
     * 		然后删除手工添加的第二条消息
     * 		判断第一个文件已经被删除
     * 		再做一次update
     * 		获取出来的内容是最后一次更新的内容。		
     * 
     * @throws Exception
     */
    @Test
    public void testAdd_UpdateInDifferentFile1() throws Exception {
    	byte[] key = UniqId.getInstance().getUniqIDHash();
    	byte[] key2 = UniqId.getInstance().getUniqIDHash();
    	store.add(key, "OriginalData".getBytes());
    	store.add(key2, "SecondData".getBytes());

    	final int messageLength = 1024 * 1024 * 8;  	
    	final int count = JournalStore.FILE_SIZE / messageLength + 3;
    	
    	for(int i = 0; i < count; i++){
    		byte[] data = new byte[messageLength];
    		byte[] k = UniqId.getInstance().getUniqIDHash();
    		store.add(k, data);
    		store.remove(k);
    	}    	    	
    	
    	store.update(key, "FirstUpdate".getBytes());
   	
    	//检测日志文件
    	RandomAccessFile f = new RandomAccessFile(getFilePrefix() + "1.log", "r");
    	f.seek(f.length() - OpItem.LENGTH);
    	byte[] opItem = new byte[OpItem.LENGTH];
    	f.read(opItem);
    	f.close();
    	
    	byte[] keyRead = Arrays.copyOf(opItem, 16);
    	Assert.assertTrue(new BytesKey(key).equals(new BytesKey(keyRead)));
    	assertEquals(OpItem.OP_DEL, opItem[16]);
    	assertEquals(0, opItem[17]);
    	assertEquals(0, opItem[18]);
    	assertEquals(0, opItem[19]);
    	assertEquals(1, opItem[20]);
    	   	
    	store.remove(key2);
    	
    	Assert.assertFalse(new File(getFilePrefix() + "1").exists());
    	Assert.assertFalse(new File(getFilePrefix() + "1.log").exists());

    	store.update(key, "LastUpdate".getBytes());
    	assertEquals(0, "LastUpdate".compareTo(new String(store.get(key))));

    }    
    
    /**
     * 测试add和update不在同一个文件中的情况。update在同一个文件中
     * 		添加一个message，然后再添加另外一个message，然后Update第一个message然后填满这个文件。	
     * 		做1次update
     * 		检测第一个文件的日志的最后是一个删除记录
     * 		然后删除手工添加的第二条消息
     * 		判断第一个文件已经被删除
     * 		再做一次update
     * 		获取出来的内容是最后一次更新的内容。		
     * 
     * @throws Exception
     */
    @Test
    public void testAdd_UpdateInDifferentFile2() throws Exception {
    	byte[] key = UniqId.getInstance().getUniqIDHash();
    	byte[] key2 = UniqId.getInstance().getUniqIDHash();
    	store.add(key, "OriginalData".getBytes());
    	store.add(key2, "SecondData".getBytes());
    	store.update(key, "FirstUpdate".getBytes());
    	store.update(key, "SecondUpdate".getBytes());

    	final int messageLength = 1024 * 1024 * 8;  	
    	final int count = JournalStore.FILE_SIZE / messageLength + 3;
    	
    	for(int i = 0; i < count; i++){
    		byte[] data = new byte[messageLength];
    		byte[] k = UniqId.getInstance().getUniqIDHash();
    		store.add(k, data);
    		store.remove(k);
    	}    	    	
    	
    	store.update(key, "FirstUpdate".getBytes());
   	
    	//检测日志文件
    	RandomAccessFile f = new RandomAccessFile(getFilePrefix() + "1.log", "r");
    	f.seek(f.length() - OpItem.LENGTH);
    	byte[] opItem = new byte[OpItem.LENGTH];
    	f.read(opItem);
    	f.close();
    	
    	byte[] keyRead = Arrays.copyOf(opItem, 16);
    	Assert.assertTrue(new BytesKey(key).equals(new BytesKey(keyRead)));
    	assertEquals(OpItem.OP_DEL, opItem[16]);
    	assertEquals(0, opItem[17]);
    	assertEquals(0, opItem[18]);
    	assertEquals(0, opItem[19]);
    	assertEquals(1, opItem[20]);
    	   	
    	store.remove(key2);
    	
    	Assert.assertFalse(new File(getFilePrefix() + "1").exists());
    	Assert.assertFalse(new File(getFilePrefix() + "1.log").exists());

    	store.update(key, "LastUpdate".getBytes());
    	assertEquals(0, "LastUpdate".compareTo(new String(store.get(key))));

    }        
    
    /**
     * 测试在跨文件Update数据失败的恢复。
     * 可能出现的情况是，Update的时候，写入了新的数据，而老的数据没有加入Remove，测试在重启的时候进行恢复。
     * 		先向第一个文件写入一个消息，再写入一个消息
     * 		填满这个文件。
     * 		确认两个文件都存在
     * 		关闭store
     * 		手工在第二个文件中写入数据
     * 		启动store
     * 		判断第一个文件最后增加了一个REMOVE的日志
     * 		删除第二个消息
     * 		判断第一个文件被删除
     * 		填满第二个文件
     * 		删除第一个消息
     * 		判断第二个文件被删除
     * @throws Exception
     */
    @Test
    public void testBrokenUpdate() throws Exception {
    	byte[] key = UniqId.getInstance().getUniqIDHash();
    	byte[] key2 = UniqId.getInstance().getUniqIDHash();
    	
    	store.add(key, "OriginalData".getBytes());
    	store.add(key2, "SecondData".getBytes());

    	final int messageLength = 1024 * 1024 * 8;  	
    	final int count = JournalStore.FILE_SIZE / messageLength + 3;
    	
    	for(int i = 0; i < count; i++){
    		byte[] data = new byte[messageLength];
    		byte[] k = UniqId.getInstance().getUniqIDHash();
    		store.add(k, data);
    		store.remove(k);
    	}
    	
    	Assert.assertTrue(new File(getFilePrefix() + "1").exists());
    	Assert.assertTrue(new File(getFilePrefix() + "1.log").exists());
    	Assert.assertTrue(new File(getFilePrefix() + "2").exists());
    	Assert.assertTrue(new File(getFilePrefix() + "2.log").exists());
    	
    	after();
    	
    	//在第二个文件写入Add 消息的信息
    	RandomAccessFile df = new RandomAccessFile(getFilePrefix() + "2", "rw");
    	RandomAccessFile lf = new RandomAccessFile(getFilePrefix() + "2.log", "rw");
    	add(df, lf, key, "FirstUpdate".getBytes(), 2);
    	
    	df.close();
    	lf.close();
    	
    	store = new JournalStore(getPath(), getStoreName());
    	
    	//检测日志文件
    	RandomAccessFile f = new RandomAccessFile(getFilePrefix() + "1.log", "r");
    	f.seek(f.length() - OpItem.LENGTH);
    	byte[] opItem = new byte[OpItem.LENGTH];
    	f.read(opItem);
    	f.close();

    	store.remove(key2);
    	Assert.assertFalse(new File(getFilePrefix() + "1").exists());
    	Assert.assertFalse(new File(getFilePrefix() + "1.log").exists());
    	
    	for(int i = 0; i < count; i++){
    		byte[] data = new byte[messageLength];
    		byte[] k = UniqId.getInstance().getUniqIDHash();
    		store.add(k, data);
    		store.remove(k);
    	}

    	store.remove(key);
    	Assert.assertFalse(new File(getFilePrefix() + "2").exists());
    	Assert.assertFalse(new File(getFilePrefix() + "2.log").exists());   	
    }
    
    
    /**
     * 测试log文件不完整的情况
     * 在有些情况下，Index没有完整写入机器就挂掉了
     * 	写入一个完成的信息，再写入一个不完整的log
     * 	创建store
     * 	读入数据正常
     * 	测试log文件的长度为一个log的长度
     * @throws Exception
     */
    @Test
    public void testBrokenIndex() throws Exception {
    	after();
    	RandomAccessFile df = new RandomAccessFile(getFilePrefix() + "1", "rw");
    	RandomAccessFile lf = new RandomAccessFile(getFilePrefix() + "1.log", "rw");
    	
    	byte[] key = UniqId.getInstance().getUniqIDHash(); 
    	byte[] key2 = UniqId.getInstance().getUniqIDHash();
    	add(df, lf, key, "Message".getBytes(), 1);
    	lf.write(key2);
    	
    	df.close();
    	lf.close();
    	
    	store = new JournalStore(getPath(), getStoreName());
    	assertEquals(0, "Message".compareTo(new String(store.get(key))));
    	after();
    	
    	lf = new RandomAccessFile(getFilePrefix() + "1.log", "r");
    	assertEquals(OpItem.LENGTH, lf.length());
    	lf.close();
    }
    
    private void remove(RandomAccessFile lf, byte[] key, long offset, int number, int dataLength) 
			throws IOException {
		lf.write(key);
		lf.write(OpItem.OP_DEL);
		lf.writeInt(number);
		lf.writeLong(offset);
		lf.writeInt(dataLength);
	}

	private long add(RandomAccessFile df, RandomAccessFile lf, byte[] key, byte[] data, int number)
			throws IOException {
		df.write(data);
		lf.write(key);
		lf.write(OpItem.OP_ADD);
		lf.writeInt(number);
		lf.writeLong(df.length() - data.length);
		lf.writeInt(data.length);
		return df.length() - data.length;
	}

    @Test
    public void testAddGetRemoveMixed() throws Exception {
        long s = System.currentTimeMillis();
        byte[] key = UniqId.getInstance().getUniqIDHash();
        for (int k = 0; k < 10000; ++k) {
            this.store.add(key, "hellofdfdfdfdfd".getBytes());
            byte[] data = this.store.get(key);
            assertNotNull(data);
            assertEquals("hellofdfdfdfdfd", new String(data));
            assertEquals(1, store.size());
            this.store.remove(key);
            assertEquals(0, store.size());
            data = this.store.get(key);
            assertNull(data);
            assertEquals(0, store.size());
        }
        System.out.println((System.currentTimeMillis() - s) + "ms");
    }

    @Test
    public void testLoadAddReadRemove10K() throws Exception {
        loadAddReadRemove(getMsg10K());
    }

    @Test
    public void testLoadAddReadRemove1K() throws Exception {
        loadAddReadRemove(getMsg1K());
    }

    public void loadAddReadRemove(String msg) throws Exception {
        int num = 100000;
        //load add
        long s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.add(getId(k, k), msg.getBytes());
        }
        s = System.currentTimeMillis() - s;
        System.out.println("add " + msg.getBytes().length + " bytes " + num
                + " times waste " + s + "ms, average " + s * 1.0d / num);
        assertEquals(num, store.size());

        //load read
        s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.get(getId(k, k));
        }
        s = System.currentTimeMillis() - s;
        System.out.println("get " + msg.getBytes().length + " bytes " + num
                + " times waste " + s + "ms, average " + s * 1.0d / num);

        //load remove
        s = System.currentTimeMillis();
        for (int k = 0; k < num; k++) {
            this.store.remove(getId(k, k));
        }
        s = System.currentTimeMillis() - s;
        System.out.println("remove " + num + " times waste " + s
                + "ms, average " + s * 1.0d / num);
        assertEquals(0, store.size());
    }

    @Test
    public void testLoadHeavy() throws Exception {
        load(8, 2000, 5);
    }

    @Test
    public void testLoadMin() throws Exception {
        load(2, 2000, 5);
    }

    public void load(int ThreadNum, int totalPerThread, long meantime)
            throws Exception {
        MsgCreator[] mcs = new MsgCreator[ThreadNum];
        MsgRemover[] mrs = new MsgRemover[mcs.length];
        for (int i = 0; i < mcs.length; i++) {
            MsgCreator mc = new MsgCreator(i, totalPerThread, meantime);
            mcs[i] = mc;
            mc.start();
            MsgRemover mr = new MsgRemover(i, totalPerThread, meantime);
            mrs[i] = mr;
            mr.start();
        }

        for (int i = 0; i < mcs.length; i++) {
            mcs[i].join();
            mrs[i].join();
        }

        assertEquals(0, store.size());

        long totalAddTime = 0;
        long totalRemoveTime = 0;
        for (int i = 0; i < mcs.length; i++) {
            totalAddTime += mcs[i].timeTotal;
            totalRemoveTime += mrs[i].timeTotal;
        }

        System.out.println(totalPerThread * ThreadNum * 2 + " of " + ThreadNum
                * 2 + " thread average: add " + totalAddTime * 1.0d
                / (totalPerThread * ThreadNum) + ", remove " + totalRemoveTime
                * 1.0d / (totalPerThread * ThreadNum));
    }

    static byte[] getId(int id, int seq) {
        final byte tmp[] = new byte[16];
        tmp[0] = (byte) ((0xff000000 & id) >> 24);
        tmp[1] = (byte) ((0xff0000 & id) >> 16);
        tmp[2] = (byte) ((0xff00 & id) >> 8);
        tmp[3] = (byte) (0xff & id);

        tmp[4] = (byte) ((0xff000000 & seq) >> 24);
        tmp[5] = (byte) ((0xff0000 & seq) >> 16);
        tmp[6] = (byte) ((0xff00 & seq) >> 8);
        tmp[7] = (byte) (0xff & seq);
        return tmp;
    }

    private static final byte[] MSG_BYTES = new byte[102400];

    private String getMsg1K() {
        return new String(MSG_BYTES, 0, 1024);
    }

    private String getMsg10K() {
        return new String(MSG_BYTES, 0, 10240);
    }

    private class MsgCreator extends Thread {
        int id;

        int totalPerThread;

        long timeTotal;

        long meantime;

        MsgCreator(int id, int totalPerThread, long meantime) {
            this.id = id;
            this.totalPerThread = totalPerThread;
            this.meantime = meantime;
        }

        public void run() {
            for (int k = 0; k < totalPerThread; k++) {
                try {
                    Thread.sleep(meantime);
                    long start = System.currentTimeMillis();
                    store.add(JournalStoreTest.getId(id, k), getMsg1K()
                            .getBytes());
                    timeTotal += System.currentTimeMillis() - start;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private class MsgRemover extends Thread {
        int id;

        int totalPerThread;

        long timeTotal;

        long meantime;

        MsgRemover(int id, int totalPerThread, long meantime) {
            this.id = id;
            this.totalPerThread = totalPerThread;
            this.meantime = meantime;
        }

        public void run() {
            for (int k = 0; k < totalPerThread;) {
                try {
                    Thread.sleep(meantime);
                    byte[] read = store.get(JournalStoreTest.getId(id, k));
                    if (read == null) {
                        continue;
                    }
                    long start = System.currentTimeMillis();
                    boolean success = store.remove(JournalStoreTest
                            .getId(id, k));
                    timeTotal += System.currentTimeMillis() - start;
                    if (!success) {
                        throw new IllegalStateException();
                    }
                    k++;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }
}
