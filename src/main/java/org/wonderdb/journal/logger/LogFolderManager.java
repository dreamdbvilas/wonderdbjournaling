package org.wonderdb.journal.logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.wonderdb.journal.logger.LogApplier.Event;
import org.wonderdb.journal.logger.LogApplier.eventType;
import org.wonderdb.journal.logger.LogManager.TxnData;

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

class LogFolderManager {
	SortedMap<Long, FileChannel> notAppliedRedoFileMap = new TreeMap<Long, FileChannel>();
	String[] logFolderPath = null;
	FileChannel currentFile = null;
	long maxFileSize = -1;
	int currentFolderPosn = 0;
	int currentFilePosn = 0;
	
	private static LogFolderManager instance = new LogFolderManager();
	
	private LogFolderManager() {
	}

	public static LogFolderManager getInstance() {
		return instance;
	}
	
	public void shutdown() {
		notAppliedRedoFileMap.put(Long.MAX_VALUE, currentFile);
		
		try {
			applyNow();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		Iterator<FileChannel> iter = notAppliedRedoFileMap.values().iterator();
		while (iter.hasNext()) {
			FileChannel channel = iter.next();
			try {
				channel.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public synchronized void log(ConcurrentLinkedQueue<TxnData> txnData) throws IOException {
		Iterator<TxnData> iter = txnData.iterator();
		List<ChannelBuffer> list = new ArrayList<>();
		ChannelBuffer buffer = ChannelBuffers.buffer(Long.SIZE/8+Integer.SIZE/8);
		buffer.writerIndex(buffer.capacity());
		list.add(buffer);
		
		while (iter.hasNext()) {
			TxnData data = iter.next();
			Byte b = LogMetaMgr.getInstance().getFileId(data.fileName);
			buffer = ChannelBuffers.buffer(1+Long.SIZE/8+Integer.SIZE/8);
			list.add(buffer);
			buffer.writeByte(b);
			buffer.writeLong(data.posn);
			buffer.writeInt(data.buffer.capacity());
			data.buffer.clear();
			data.buffer.writerIndex(data.buffer.capacity());
			list.add(data.buffer);
		}
		ChannelBuffer[] buffers = new ChannelBuffer[list.size()];
		buffers = list.toArray(buffers);
		ChannelBuffer wrappedBuf = ChannelBuffers.wrappedBuffer(buffers);
		wrappedBuf.clear();
		
		long txnTime = System.currentTimeMillis();
		wrappedBuf.writeLong(txnTime);
		wrappedBuf.writeInt(wrappedBuf.capacity()-(Integer.SIZE/8+Long.SIZE/8));
		wrappedBuf.clear();
		wrappedBuf.writerIndex(wrappedBuf.capacity());	
		ByteBuffer buf = wrappedBuf.toByteBuffer();
		currentFile.write(buf, currentFile.size());
		Event event = new Event();
		event.type = eventType.commit;
		event.data = wrappedBuf;
		LogApplier.getInstance().logEvent(event);
		
		if (currentFile.size() > maxFileSize ) {
			setNewCurrentFile(txnTime);
			event = new Event();
			event.type = eventType.logSwitch;
			LogApplier.getInstance().logEvent(event);
		}
	}
	
	private int pickLogFolder() {
		if (logFolderPath.length == 1) {
			return 0;
		}
		
		for (int i = 0; i < logFolderPath.length; i++) {
			if (i == currentFolderPosn) {
				continue;
			} else {
				return i;
			}
		}
		
		return -1;
	}
	
	private void setNewCurrentFile(long lastTxnTime) throws IOException {
		updateLastCommitTime(currentFile, lastTxnTime);
		notAppliedRedoFileMap.put(lastTxnTime, currentFile);
		
		long lastSyncTime = LogMetaMgr.getInstance().getSyncTime();
		
		int folderPosn = pickLogFolder();
		int fileCount = 0;
		
		while (true) {
			if (folderPosn == currentFolderPosn && fileCount == currentFilePosn) {
				fileCount++;
				continue;
			}
			
			String logFileName = logFolderPath[folderPosn] + "/redolog" + fileCount;
			Path path = Paths.get(logFileName);
			FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
			lastTxnTime = getLastCommitTime(channel);
			if (lastSyncTime > lastTxnTime) {
				currentFile = channel;
				currentFilePosn = fileCount;
				currentFolderPosn = folderPosn;
				updateLastCommitTime(currentFile, Long.MAX_VALUE);
				currentFile.truncate(Long.SIZE/8);
				currentFile.position(currentFile.size());
				break;
			} else {
				channel.close();
				fileCount++;
			}
		}
	}
	
	private void updateLastCommitTime(FileChannel channel, long time) throws IOException {
		ChannelBuffer buffer = ChannelBuffers.buffer(Long.SIZE/8);
		buffer.writeLong(time);
		channel.write(buffer.toByteBuffer(), 0);
	}
	
	
	public void init(String[] folderPath, int maxFileSize) throws IOException {
		this.logFolderPath = folderPath;
		this.maxFileSize = maxFileSize;
		
		init(LogMetaMgr.getInstance().getSyncTime());
	}
	
	private void init(long time) throws IOException {
		Path path = null;
		FileChannel channel = null;
		
		for (int i = 0; i < logFolderPath.length; i++) {
			init(time, logFolderPath[i]);
		}

		Iterator<Long> iter = notAppliedRedoFileMap.keySet().iterator();
		while (iter.hasNext()) {
			long syncTime = iter.next();
			channel = notAppliedRedoFileMap.get(syncTime);
			long s = channel.size();
			sync(channel, time, s);
			channel.truncate(0);
			channel.force(true);
			channel.close();
		}
		
		notAppliedRedoFileMap.clear();
		path = Paths.get(logFolderPath[0] + "/redolog0");
		channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
		currentFile = channel;
		updateLastCommitTime(channel, Long.MAX_VALUE);

	}
	
	private void init(long time, String logPath) throws IOException {
		Path path = null;
		FileChannel channel = null;
		
		File file = new File(logPath);
		
		if (!file.exists() ) {
			file.mkdir();
		}

		if (file.isDirectory()) {
			String[] files = file.list();
			for (int i = 0; i < files.length; i++) {
				String s = files[i];
				if (s != null && s.contains("redolog")) {
					path = Paths.get(s);
					channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
					long lastCommitTime = getLastCommitTime(channel);
					if (lastCommitTime >= time) {
						notAppliedRedoFileMap.put(lastCommitTime, channel);
					}
				}
			}
		}
	}
	
	private long getLastCommitTime(FileChannel channel) throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(Long.SIZE/8);
		long retVal = Long.MIN_VALUE;
		
		if (channel.size() >= Long.SIZE/8) {
			channel.read(buf, 0);
			buf.flip();
			ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buf);
			buffer.clear();
			buffer.writerIndex(buffer.capacity());
			retVal = buffer.readLong();
		}
		
		return retVal;
	}
	
	void apply(ByteBuffer data) throws IOException {
		data.flip();
		data.limit(data.capacity());
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(data);
		buffer.clear();
		buffer.writerIndex(buffer.capacity());
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
		
		while (buffer.readableBytes() > 0) {
			byte fileId = buffer.readByte();
			long posn = buffer.readLong();
			int count = buffer.readInt();
			byte[] bytes = new byte[count];
			ByteBuffer buf = ByteBuffer.wrap(bytes);
			buffer.readBytes(bytes);
			AsynchronousFileChannel channel = LogMetaMgr.getInstance().getFileChannel(fileId);
			Future<Integer> future = channel.write(buf, posn);
			futures.add(future);
		}
		
		for (int i = 0; i < futures.size(); i++) {
			try {
				futures.get(i).get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void sync(FileChannel channel, long syncTime, long upto) throws IOException {
		long channelPosn = Long.SIZE/8;
		
		long time = -1;
		while (channelPosn < upto) {
			ByteBuffer buf = ByteBuffer.allocate(Long.SIZE/8+Integer.SIZE/8);
			channel.read(buf, channelPosn);
			buf.flip();
			channelPosn = channelPosn + buf.capacity();
			ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buf);
			buffer.clear();
			buffer.writerIndex(buffer.capacity());
			
			time = buffer.readLong();
			int size = buffer.readInt();
			
			if (time >= syncTime) {
				ByteBuffer buf1 = ByteBuffer.allocate(size);
				channel.read(buf1, channelPosn);
				apply(buf1);
			}
			channelPosn = channelPosn + size;
		}
	}
	
	public void applyNow() throws IOException {
		List<FileChannel> filesToApply = null;
		synchronized (this) {
			filesToApply = new ArrayList<FileChannel>(notAppliedRedoFileMap.values());
		}
		long lastSyncTime = LogMetaMgr.getInstance().getSyncTime();
		
		for (int i = 0; i < filesToApply.size(); i++) {
			FileChannel channel = filesToApply.get(i);
			long lastCommitTime = getLastCommitTime(channel);
			if (lastCommitTime >= lastSyncTime) {
				sync(channel, lastSyncTime, channel.size());
				LogMetaMgr.getInstance().updateLastSyncTime(lastCommitTime);
			}
		}
		
		long endPosn = -1;
		synchronized (this) {
			endPosn = currentFile.size();
		}
		sync(currentFile, lastSyncTime, endPosn);
	}
}
