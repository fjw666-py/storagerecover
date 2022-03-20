package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.driver.*;
import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

import javax.swing.text.html.HTML;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}

	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;

	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private StorageManager storageManager;

	private LogManager logManager;

	/**
	 * 从截止位置开始的，没有结束的事务集合
	 */
	private HashMap<Long,HashMap<Long,Integer>> txKeyTag;

	/**
	 * 从截止位置开始，提交了但是没有结束的事务集合
	 */
	private HashSet<Long> com;
//	private Deque<Commit> com;
	/**
	 * 从截止位置开始的，没有结束的事务集合的开始位置
	 */
	private HashMap<Long,Integer> startPos;


	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = null;

		txKeyTag = new HashMap<>();

		com = new HashSet<>();

		startPos = new HashMap<>();

	}

	public void setStorageManager(StorageManager storageManager) {
		this.storageManager = storageManager;
	}

	public void setLogManager(LogManager logManager){
		this.logManager = logManager;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */

	/*
	准备事务管理器，以服务于操作。
	*此时，您应该检测StorageManager是否不一致，并将其恢复。
	 */
	//初始化之后就被调用，首先将storageManager中的内容加载到lastestValue中去
	public void initAndRecover(StorageManager sm, LogManager lm) {
		//首先加入的是最早部分的信息。
		setStorageManager(sm);
		setLogManager(lm);
		latestValues = storageManager.readStoredTable();

		Integer lastTruncate = logManager.getLogTruncationOffset();
		Integer endOffset = logManager.getLogEndOffset();
		//可以迁移到初始化的部分
		Queue<Forecover> queue = new ArrayDeque<>();
		while(lastTruncate < endOffset){
			//查看事务的id
			ByteBuffer byteBuffer = ByteBuffer.wrap(logManager.readLogRecord(lastTruncate,Long.BYTES*2+Byte.BYTES*2));
			Long txid =byteBuffer.getLong();
			byte state = byteBuffer.get();
			Long cur_key = byteBuffer.getLong();
			byte lengthOfValue = byteBuffer.get();
			byte length = 0;
			if(state == 2) {
				byte[] value = logManager.readLogRecord(lastTruncate + Long.BYTES * 2 + Byte.BYTES * 2, lengthOfValue);
				//如果不包括的话，一定是上一个truncate之前已经end的事务剩下的事务。不需要在txKeyMap中添加
				if(startPos.containsKey(txid)) {
					Forecover forecover = new Forecover(txid,cur_key,lastTruncate,value);
					queue.add(forecover);
					HashMap<Long, Integer> new_map = txKeyTag.getOrDefault(txid, new HashMap<>());
					new_map.put(cur_key, lastTruncate);
					txKeyTag.put(txid, new_map);
				}
				length = (byte) (Long.BYTES * 2 + Byte.BYTES * 2 + lengthOfValue);
			}else{
				if(state == 1){
					startPos.put(txid,lastTruncate);
				}
				if(state == 3){
					if(startPos.containsKey(txid)) {
						com.add(txid);
					}
				}
				if(state==4){
					if(startPos.containsKey(txid)){
						startPos.remove(txid);
					}
					if(txKeyTag.containsKey(txid)){
						txKeyTag.remove(txid);
					}
					com.remove(txid);
				}
				length =(byte)(Long.BYTES*2 + Byte.BYTES*2);
			}
			lastTruncate += length;
		}

		while(queue.size()!=0){
			Forecover peek = queue.peek();
			if(com.contains(peek.getTxid())){
				if(!latestValues.containsKey(peek.getKey()) || latestValues.containsKey(peek.getKey()) &&
						latestValues.get(peek.getKey()).tag < peek.getTag()) {
					latestValues.put(peek.getKey(), new TaggedValue(peek.getTag(), peek.getValue()));
					storageManager.queueWrite(peek.getKey(), peek.getTag(), peek.getValue());
				}
			}
			queue.poll();
		}
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
//	指示新事务的开始。我们将保证txID始终增加（即使在发生碰撞时）
	//tx指示事务的id
	//实现方式，读取lastestValues中的值，然后
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
//		TODO：不针对非持久事务实现，您应该实现这一点
		Record start = new Record(txID,(byte)1);
		byte[] serialize = start.serialize();
		int start_pos = logManager.appendLogRecord(serialize);
		startPos.put(txID,start_pos);
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
//	返回最近事务为密钥提交的最新值
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	//	指示对数据库的写入。请注意，这样的写入不应该对read（）可见
	//*调用，直到进行写操作的事务提交。为了简单起见
	//在对密钥进行写操作之后，我们不会从txID本身读取同一个密钥
	public void write(long txID, long key, byte[] value) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));
	}

	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.
	 */
	//	提交事务，并使其写操作对后续读取操作可见。
	public void commit(long txID) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		Queue<Forcommit> forcommits = new ArrayDeque<>();
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				//添加到logmanager中去
				Record record= new Record(txID,(byte)2,x.key,(byte)x.value.length, x.value);
				byte[] serialize = record.serialize();
				int pos = logManager.appendLogRecord(serialize);
				latestValues.put(x.key,new TaggedValue(pos,x.value));
				forcommits.add(new Forcommit(x.key,pos,x.value));
			}
		}
		//在最后添加一条commit的记录到logmanager中去。
		Record comm = new Record(txID,(byte)3);
		byte[] commit = comm.serialize();
		logManager.appendLogRecord(commit);
		com.add(txID);
		if (writeset != null) {
			while(forcommits.size()!=0){
				Forcommit peek = forcommits.peek();
				storageManager.queueWrite(peek.getKey(),peek.getTag(),peek.getValue());
				HashMap<Long,Integer> map = txKeyTag.getOrDefault(txID,new HashMap<>());
				map.put(peek.getKey(),peek.getTag());
				txKeyTag.put(txID,map);
				forcommits.poll();
			}
		}
		writesets.remove(txID);
	}

	/**
	 * Aborts a transaction.
	 */
	//回滚一个事务
	public void abort(long txID) {
		writesets.remove(txID);
		startPos.remove(txID);
		txKeyTag.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
//	每当队列写入变为持久时，存储管理器都会调用此过程。
//	*这些调用是按写入密钥的顺序进行的，除非发生崩溃，否则每一次这样的排队写入都会发生一次。
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		HashSet<Long> alreadyPersist = new HashSet<>();
		for(Long txn: com){
			if(txKeyTag.containsKey(txn)){
				if(txKeyTag.get(txn).containsKey(key)
						&& txKeyTag.get(txn).get(key) <= persisted_tag){
					txKeyTag.get(txn).remove(key);
				}
			}
			if(txKeyTag.get(txn).size() ==0){
				alreadyPersist.add(txn);
				startPos.remove(txn);
			}
		}
		//设置end标志
// 		for(Long txn:alreadyPersist){
// 			com.remove(txn);
// 			Record comm = new Record(txn,(byte)4);
// 			byte[] commit = comm.serialize();
// 			logManager.appendLogRecord(commit);
// 		}
// 		//设置截断策略
// 		boolean flag = true;
// 		for(Map.Entry<Long ,Integer> entry : startPos.entrySet()){
// 			if(entry.getValue() <= persisted_tag){
// 				flag = false;
// 				break;
// 			}
// 		}
// 		if(flag){
// 			logManager.setLogTruncationOffset((int)persisted_tag);
// 		}
		
		Integer truncate = 0;
		for(Long txn:alreadyPersist){
			com.remove(txn);
			Record comm = new Record(txn,(byte)4);
			byte[] commit = comm.serialize();
			truncate = logManager.appendLogRecord(commit);
		}

		//设置截断策略
		if(alreadyPersist.size()!=0) {
			boolean flag = true;
			for (Map.Entry<Long, Integer> entry : startPos.entrySet()) {
				if (entry.getValue() <= truncate) {
					flag = false;
					break;
				}
			}
			if (flag) {
				logManager.setLogTruncationOffset((int) truncate);
			}
		}
	}
}
