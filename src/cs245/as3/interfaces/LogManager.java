package cs245.as3.interfaces;

public interface LogManager {
	// During testing, all methods of LogManager might throw
        // CrashException, which is a custom RuntimeException subclass we
        // are using for testing and simulating crashes. You are not expected to
	// catch any of these. Simply allow them to be caught by the test driver.

	//在测试期间，LogManager的所有方法都可能抛出
	//CrashexException，这是一个定制的RuntimeException子类，我们
	//用于测试和模拟碰撞。你不应该
	//抓住任何一个。只需让他们被测试驾驶员抓住即可。

	/**
	 * @return the offset of the end of the log
	 */
	public int getLogEndOffset();


	/**
	 * Reads from log at the specified position.
	 * @return bytes in the log record in the range [offset, offset + size)
	 */
	/*
	在指定位置读取日志。
	*@返回日志记录中[offset，offset+size]范围内的字节数
	 */
	public byte[] readLogRecord(int offset, int size);


	/**
	 * Atomically appends and persists record to the end of the log (implying that all previous appends have succeeded).
	 * @param record
	 * @return the log length prior to the append
	 */
	/*
	 *以原子方式将记录追加并持久化到日志末尾（这意味着之前的所有追加都已成功）。
	 *@param记录
	 *@返回追加之前的日志长度
	 */
	public int appendLogRecord(byte[] record);


	/**
	 * @return the current log truncation offset
	 */
	//	当前日志截断偏移量
	public int getLogTruncationOffset();

	/**
	 * Durably stores the offset as the current log truncation offset and truncates (deletes) the log up to that point.
	 * You can assume this occurs atomically. The test code will never call this.
	 */
//	*持久地将偏移量存储为当前日志截断偏移量，并截断（删除）日志直至该点。
//			*你可以假设这是原子性的。测试代码永远不会调用它。
	public void setLogTruncationOffset(int offset);
}
