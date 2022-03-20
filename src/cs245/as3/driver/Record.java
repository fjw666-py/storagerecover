package cs245.as3.driver;

import cs245.as3.TransactionManager;

import java.nio.ByteBuffer;

public class Record {
    /**
     * 事务状态
     */
    long txID;
    //设置事务的状态： 1为start, 2为写入数据, 3为commit,4为end;
    byte state;
    long key;
    byte length;
    byte[] value;

    public long getTxID() {
        return txID;
    }

    public void setTxID(long txID) {
        this.txID = txID;
    }

    public byte getState() {
        return state;
    }

    public void setState(byte state) {
        this.state = state;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public byte getLength() {
        return length;
    }

    public void setLength(byte length) {
        this.length = length;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Record(){
        this.txID = 0;
        this.key = 0;
        this.length = 0;
        this.value = null;
    }

    public Record(long txID, byte state){
        this.txID = txID;
        this.state = state;
        this.key = -1;
        this.length = -1;
    }

    public Record(long txID,byte state,long key,byte length, byte[] value) {
        this.txID = txID;
        this.state = state;
        this.length = length;
        this.key = key;
        this.value = value;
    }

    public byte[] serialize() {
        //申请一个Long的字节数（Long.BYTES=SIZE(Long中的静态变量=64）/Byte.SIZE）+value的字节数的内存空间
        if(this.state == 2) {
            ByteBuffer ret = ByteBuffer.allocate(Long.BYTES*2 + Byte.BYTES*2 + value.length);
            ret.putLong(txID);
            ret.put(state);
            ret.putLong(key);
            ret.put(length);
            ret.put(value);
            return ret.array();
        }else{
            ByteBuffer ret = ByteBuffer.allocate(Long.BYTES*2 + Byte.BYTES*2);
            ret.putLong(txID);
            ret.put(state);
            ret.putLong(key);
            ret.put(length);
            return  ret.array();
        }
    }

    static Record deserialize(byte[] b) {
        ByteBuffer bb = ByteBuffer.wrap(b); //将字节数组b包装到缓冲区中
        long txID = bb.getLong();
        byte state = bb.get();
        if(state == 2) {
            long key = bb.getLong(); // 读取一个Long长的值
            byte length = bb.get();
            byte[] value = new byte[length];
            bb.get(value);
            return new Record(txID,state,key,length,value);
        }else return new Record(txID,state);
    }
}
