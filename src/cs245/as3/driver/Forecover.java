package cs245.as3.driver;

public class Forecover {
    Long txid;
    Long key;
    Integer tag;
    byte[] value;
    public Forecover(Long txid,Long key, Integer tag, byte[] value){
        this.txid = txid;
        this.tag = tag;
        this.key = key;
        this.value = value;
    }

    public Long getKey() {
        return key;
    }

    public void setKey(Long key) {
        this.key = key;
    }

    public Integer getTag() {
        return tag;
    }

    public void setTag(Integer tag) {
        this.tag = tag;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setTxid(Long txid) {
        this.txid = txid;
    }

    public Long getTxid() {
        return txid;
    }
}
