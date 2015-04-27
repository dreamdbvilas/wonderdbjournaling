package org.wonderdb.journal.logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.wonderdb.journal.logger.LogManager.applyType;

public class LogApplier {
	private static LogApplier instance = new LogApplier();
	enum eventType { commit, logSwitch, Shutdown };
	
	Thread processorThread = null;
	applyType type = applyType.OnLogSwitch;
	int soOftenFrequency = -1;
	List<Event> eventList = new ArrayList<Event>();
	BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>();
	
	private LogApplier() {
	}
	
	public static LogApplier getInstance() {
		return instance;
	}
	
	public void init(applyType type, int applyFrequency) {
		this.type = type;
		this.soOftenFrequency = applyFrequency;
		ProcessorRunnable runnable = new ProcessorRunnable();
		processorThread = new Thread(runnable);
		processorThread.start();
	}
	
	public void logEvent(Event event) {
		queue.add(event);
	}
	
	void applyCommit(Event event, boolean updateLasySyncTime) throws IOException {
		ChannelBuffer buffer = (ChannelBuffer) event.data;
		buffer.clear();
		buffer.writerIndex(buffer.capacity());
		long commitTime = buffer.readLong();
		int size = buffer.readInt();
		ByteBuffer buf = buffer.toByteBuffer(buffer.readerIndex(), buffer.readableBytes());
		LogFolderManager.getInstance().apply(buf);
		if (updateLasySyncTime) {
			LogMetaMgr.getInstance().updateLastSyncTime(commitTime);
		}
	}
	
	private void processEvent(Event event) {
		try {
			switch(type) {
				case Every:
					if (event.type == eventType.commit) {
						applyCommit(event, true);
					} 
					break;
				case OnLogSwitch:
					if (event.type == eventType.logSwitch) {
						LogFolderManager.getInstance().applyNow();
					}
					break;
				case SoOften:
					if (event.type == eventType.commit) {
						eventList.add(event);
						if (eventList.size() >= soOftenFrequency) {
							for (int i = 0; i < eventList.size()-1; i++) {
								applyCommit(eventList.get(i), false);
							}
							applyCommit(event, true);
							eventList.clear();
						}
					}
					break;
				default:
					break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public class ProcessorRunnable implements Runnable {
 
		@Override
		public void run() {
			try {
				while (true) {
					Event event = queue.take();
					if(event.type == eventType.Shutdown) {
						return;
					}
					processEvent(event);
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public static class Event {
		eventType type;
		Object data;
	}
}
