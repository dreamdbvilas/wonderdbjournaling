package org.wonderdb.journal.logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;


/*******************************************************************************
 *    Copyright 2013 Vilas Athavale
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/


public class LogMetaMgr {
	private static LogMetaMgr instance = new LogMetaMgr();
	
	private LogMetaMgr() {
	}
	
	public static LogMetaMgr getInstance() {
		return instance;
	}
	
	FileChannel syncTimeUpdater = null;
	
	String logFolderPath = null;
	ConcurrentMap<Byte, String> byteToStringMap = new ConcurrentHashMap<Byte, String>();
	ConcurrentMap<String, Byte> stringToByteMap = new ConcurrentHashMap<String, Byte>();
	ConcurrentMap<Byte, AsynchronousFileChannel> byteToChannelMap = new ConcurrentHashMap<Byte, AsynchronousFileChannel>();
	AtomicLong lastSyncTime = new AtomicLong(-1);
	
	void init(String folderPath) throws IOException {
		String metaFilePath = folderPath + "/meta";
		Path path = Paths.get(metaFilePath);
		
		syncTimeUpdater = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
//		fileMapAppender = FileChannel.open(path, StandardOpenOption.WRITE); 
		if (syncTimeUpdater.size() == 0) {
			updateLastSyncTime(Long.MIN_VALUE);
			return;
		}
		readLastSyncTime();
		ChannelBuffer buffer = ChannelBuffers.buffer((int) (syncTimeUpdater.size()-Long.SIZE/8));
		buffer.writerIndex(buffer.capacity());
		ByteBuffer buf = buffer.toByteBuffer();
		syncTimeUpdater.read(buf, Long.SIZE/8);
		buffer.clear();
		buffer.writerIndex(buffer.capacity());
		
		while (buffer.readable()) {
			byte b = buffer.readByte();
			int count = buffer.readInt();
			byte[] bytes = new byte[count];
			buffer.readBytes(bytes);
			String fileName = new String(bytes);
			byteToStringMap.put(b, fileName);
			stringToByteMap.put(fileName, b);
			Path p = Paths.get(fileName);
			AsynchronousFileChannel channel = AsynchronousFileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			byteToChannelMap.put(b, channel);
		}

	}
	
	synchronized String getFileName(byte b) {
		return byteToStringMap.get(b);
	}
	
	synchronized byte getFileId(String name) throws IOException {
		Byte b = stringToByteMap.get(name);
		if (b == null) {
			b = (byte) stringToByteMap.size();
			stringToByteMap.put(name, b);
			byteToStringMap.put(b, name);
			byte[] bytes = new byte[1 + Integer.SIZE/8 + name.getBytes().length];
			ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(bytes);
			buffer.clear();
			buffer.writeByte(b);
			buffer.writeInt(name.getBytes().length);
			buffer.writeBytes(name.getBytes());
			ByteBuffer buf = buffer.toByteBuffer();
//			buf.flip();
			long s = syncTimeUpdater.size();
			syncTimeUpdater.write(buf, s);

			Path p = Paths.get(name);
			AsynchronousFileChannel channel = AsynchronousFileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			byteToChannelMap.put(b, channel);
}
		return b;
	}
	
	synchronized long getSyncTime() {
		return lastSyncTime.get();
	}
	
	synchronized AsynchronousFileChannel getFileChannel(byte b) {
		return byteToChannelMap.get(b);
	}
	
	long readLastSyncTime() throws IOException {
		ChannelBuffer buffer = ChannelBuffers.buffer(Long.SIZE/8);
		buffer.clear();
		buffer.writerIndex(buffer.capacity());
		syncTimeUpdater.read(buffer.toByteBuffer(), 0);
		buffer.writerIndex(buffer.capacity());
		lastSyncTime.set(buffer.readLong());
		return lastSyncTime.get();
	}
	
	void updateLastSyncTime(long time) throws IOException {
		ChannelBuffer buffer = ChannelBuffers.buffer(Long.SIZE/8);
		buffer.writeLong(time);
		ByteBuffer buf = buffer.toByteBuffer();
		syncTimeUpdater.write(buf, 0);
	}
}
