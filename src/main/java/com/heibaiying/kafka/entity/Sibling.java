package com.heibaiying.kafka.entity;

public class Sibling {
    private String rfilename;
    private Long size; // 使用包装类型以便处理null值
    private String blob_id;
    private String lfs;

    // getters and setters
    public String getRfilename() {
        return rfilename;
    }

    public void setRfilename(String rfilename) {
        this.rfilename = rfilename;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public String getBlob_id() {
        return blob_id;
    }

    public void setBlob_id(String blob_id) {
        this.blob_id = blob_id;
    }

    public String getLfs() {
        return lfs;
    }

    public void setLfs(String lfs) {
        this.lfs = lfs;
    }


}
