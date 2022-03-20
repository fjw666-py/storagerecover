package cs245.as3.driver;

public class Forcommit {
    private Long key;
    private Integer tag;
    private byte[] value;

    public Forcommit(Long key, Integer tag, byte[] value){
        this.key = key;
        this.tag = tag;
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
}
