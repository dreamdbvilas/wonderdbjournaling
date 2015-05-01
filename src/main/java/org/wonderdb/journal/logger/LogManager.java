package org.wonderdb.journal.logger;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

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


public class LogManager {
	AtomicInteger txnCounter = new AtomicInteger(0);
	ConcurrentMap<Integer, ConcurrentLinkedQueue<TxnData>> inflightTxns = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<TxnData>>();
	public static enum applyType { Every, SoOften, OnLogSwitch, None };
	boolean shuttingDown = false;
	
	private static LogManager instance = new LogManager();
	
	private LogManager() {
	}
	
	public static LogManager getInstance() {
		return instance;
	}
	
	public void init(String[] logFileFolder, int maxFileSize, applyType type, int applyFrequency) throws IOException {
		LogMetaMgr.getInstance().init(logFileFolder[0]);
		LogFolderManager.getInstance().init(logFileFolder, maxFileSize);
		LogApplier.getInstance().init(type, applyFrequency);
	}
	
	public void shutdown() {
		shuttingDown = true;
		
		LogApplier.getInstance().shutdown();
		LogFolderManager.getInstance().shutdown();
		LogMetaMgr.getInstance().shutdonw();
	}
	
	public TransactionId startTxn() {
		if (shuttingDown) {
			throw new RuntimeException("Shutting down no new transactions allowed");
		}
		TransactionId id = new TransactionId(txnCounter.getAndIncrement());
		ConcurrentLinkedQueue<TxnData> q = new ConcurrentLinkedQueue<LogManager.TxnData>();
		inflightTxns.put(id.txnId, q);
		return id;
	}
	
	public void logTxn(TransactionId id, String fileName, long posn, ChannelBuffer buffer) {
		TxnData data = new TxnData();
		data.fileName = fileName;
		data.posn = posn;
		data.buffer = buffer;
		ConcurrentLinkedQueue<TxnData> q = inflightTxns.get(id.txnId);
		if (q == null) {
			throw new RuntimeException("You need to startTxn before starting to log");
		}
		
		q.add(data);
	}
	
	public void rollback(TransactionId id) {
		inflightTxns.remove(id.txnId);
	}
	
	public void commit(TransactionId id) throws IOException {
		ConcurrentLinkedQueue<TxnData> q = inflightTxns.get(id.txnId);
		if (q == null || q.isEmpty()) {
			return;
		}

		LogFolderManager.getInstance().log(q);
	}
	
	
	static class TxnData {
		String fileName;
		long posn;
		ChannelBuffer buffer;
	}
	
	public static void main(String[] args) throws IOException {
		String[] logPaths = new String[1];
		logPaths[0] = ".";
		LogManager.getInstance().init(logPaths, 100, applyType.OnLogSwitch, 2);
		TransactionId id = LogManager.getInstance().startTxn();
		ChannelBuffer buffer = ChannelBuffers.wrappedBuffer("vilas".getBytes());
		LogManager.getInstance().logTxn(id, "txn1", 10, buffer);
		buffer = ChannelBuffers.wrappedBuffer("athavale".getBytes());
		LogManager.getInstance().logTxn(id, "txn2", 100, buffer);
		LogManager.getInstance().commit(id);

		id = LogManager.getInstance().startTxn();
		buffer = ChannelBuffers.wrappedBuffer("vilas".getBytes());
		LogManager.getInstance().logTxn(id, "txn1", 20, buffer);
		buffer = ChannelBuffers.wrappedBuffer("athavale".getBytes());
		LogManager.getInstance().logTxn(id, "txn2", 200, buffer);
		LogManager.getInstance().commit(id);

		id = LogManager.getInstance().startTxn();
		buffer = ChannelBuffers.wrappedBuffer("vilas".getBytes());
		LogManager.getInstance().logTxn(id, "txn1", 30, buffer);
		buffer = ChannelBuffers.wrappedBuffer("athavale".getBytes());
		LogManager.getInstance().logTxn(id, "txn2", 300, buffer);
		LogManager.getInstance().commit(id);
		LogManager.getInstance().shutdown();
	}
}
