package cs245.as3.driver;

import java.nio.ByteBuffer;

import cs245.as3.interfaces.LogManager;

/**
 * DO NOT MODIFY THIS FILE IN THIS PACKAGE **
 * Make this an interface
 */
public class LogManagerImpl implements LogManager {
	private ByteBuffer log;
	private int logSize;
	private int logTruncationOffset; //日志截断，减少恢复时候的工作量
	private int Riop_counter;
	private int Wiop_counter;
	private int nIOSBeforeCrash;
	private boolean serveRequests;

	// 1 GB
	private static final int BUFSIZE_START = 1000000000;

	// 128 byte max record size
	private static final int RECORD_SIZE = 128;

	public LogManagerImpl() {
		log = ByteBuffer.allocate(BUFSIZE_START);
		logSize = 0;
		serveRequests = true;
	}

	/* **** Public API **** */

	public int getLogEndOffset() {
		return logSize;
	}


	public byte[] readLogRecord(int position, int size) throws ArrayIndexOutOfBoundsException {
		checkServeRequest();

		if ( position < logTruncationOffset || position+size > getLogEndOffset() ) {
			throw new ArrayIndexOutOfBoundsException("Offset " + (position+size) + "invalid: log start offset is " + logTruncationOffset +
					", log end offset is " + getLogEndOffset());
		}

		if ( size > RECORD_SIZE ) {
			throw new IllegalArgumentException("Record length " + size +
					" greater than maximum allowed length " + RECORD_SIZE);
		}

		byte[] ret = new byte[size];
		log.position(position);
		log.get(ret);
		Riop_counter++;
		return ret;
	}

	//Returns the length of the log before the append occurs, atomically
	public int appendLogRecord(byte[] record) {
		checkServeRequest();
		if ( record.length > RECORD_SIZE ) {
			throw new IllegalArgumentException("Record length " + record.length +
					" greater than maximum allowed length " + RECORD_SIZE);
		}
		synchronized(this) {
			Wiop_counter++; //写次数加一
			log.position(logSize); //日志位置调整至logSize处

			for ( int i = 0; i < record.length; i++ ) {
				log.put(record[i]);
			}

			int priorLogSize = logSize;

			logSize += record.length;
			return priorLogSize; //返回加入日志操作之前的日志位置
		}
	}

	public int getLogTruncationOffset() {
		return logTruncationOffset;
	}

	//设置新的日志截断的位置
	public void setLogTruncationOffset(int logTruncationOffset) {
		if (logTruncationOffset > logSize || logTruncationOffset < this.logTruncationOffset) {
			throw new IllegalArgumentException();
		}

		this.logTruncationOffset = logTruncationOffset;
	}


	/* **** For testing only **** */
	
	protected class CrashException extends RuntimeException {
		
	}
	/**
	 * We use this to simulate the log becoming inaccessible after a certain number of operations, 
	 * for the purposes of simulating crashes.
	 */
	//为了模拟崩溃，我们使用它来模拟经过一定次数的操作后日志变得不可访问。
	private void checkServeRequest() {
		if (nIOSBeforeCrash > 0) {
			nIOSBeforeCrash--;
			if (nIOSBeforeCrash == 0) {
				serveRequests = false;
			}
		}
		if (!serveRequests) {
			//Crash!
			throw new CrashException();
		}
   		assert(!Thread.interrupted()); //Cooperate with timeout:
	}

	protected int getIOPCount() {
		return Riop_counter + Wiop_counter;
	}

	protected void crash() {
		//将日志全部清除
		log.clear();
	}

	//设置模拟数据库崩溃之前的io次数
	protected void stopServingRequestsAfterIOs(int nIOsToServe) {
		nIOSBeforeCrash = nIOsToServe;
	}

	//重新开始服务
	protected void resumeServingRequests() {
		serveRequests = true;
		nIOSBeforeCrash = 0;
	}

}
