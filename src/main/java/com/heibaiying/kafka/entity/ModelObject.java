package com.heibaiying.kafka.entity;

import java.util.List;

public class ModelObject {
    private String modelId;
    private String sha;
    private String lastModified;
    private List<String> tags;
    private String pipeline_tag;
    private List<Sibling> siblings;
    private boolean isPrivate;
    private String author;
    private Config config;
    private String securityStatus;
    private String _id;
    private String id;
    private CardData cardData;
    private int likes;
    private int downloads;
    private String library_name;

//    build
    public ModelObject() {
    }

    // getters and setters
    public String getModelId() {
        return modelId;
    }
    public void setModelId(String modelId) {
        this.modelId = modelId;
    }
    public String getSha() {
        return sha;
    }
    public void setSha(String sha) {
        this.sha = sha;
    }
    public String getLastModified() {
        return lastModified;
    }

    public void setLastModified(String lastModified) {
        this.lastModified = lastModified;
    }
    public List<String> getTags() {
        return tags;
    }
    public void setTags(List<String> tags) {
        this.tags = tags;
    }
    public String getPipeline_tag() {
        return pipeline_tag;
    }
    public void setPipeline_tag(String pipeline_tag) {
        this.pipeline_tag = pipeline_tag;
    }
    public List<Sibling> getSiblings() {
        return siblings;
    }

    public void setSiblings(List<Sibling> siblings) {
        this.siblings = siblings;
    }
    public boolean isPrivate() {
        return isPrivate;
    }
    public void setPrivate(boolean isPrivate) {
        this.isPrivate = isPrivate;
    }
    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }
    public Config getConfig() {
        return config;
    }
    public void setConfig(Config config) {
        this.config = config;
    }
    public String getSecurityStatus() {
        return securityStatus;
    }

    public void setSecurityStatus(String securityStatus) {
        this.securityStatus = securityStatus;
    }
    public String get_id() {
        return _id;
    }
    public void set_id(String _id) {
        this._id = _id;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
//    ATTENTION: 每个object的CardData都是不同的，所以这里不需要CardData
//    public CardData getCardData() {
//        return cardData;
//    }
//    public void setCardData(CardData cardData) {
//        this.cardData = cardData;
//    }
    public int getLikes() {
        return likes;
    }
    public void setLikes(int likes) {
        this.likes = likes;
    }
    public int getDownloads() {
        return downloads;
    }
    public void setDownloads(int downloads) {
        this.downloads = downloads;
    }
    public String getLibrary_name() {
        return library_name;
    }
    public void setLibrary_name(String library_name) {
        this.library_name = library_name;
    }


}

