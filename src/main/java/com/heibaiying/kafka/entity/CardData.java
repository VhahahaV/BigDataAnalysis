package com.heibaiying.kafka.entity;

import java.util.List;

public class CardData {
    private List<String> tags;
    private String language;
    private String license;
    private List<String> datasets;

    // getters and setters
    public List<String> getTags() {
        return tags;
    }
    public void setTags(List<String> tags) {
        this.tags = tags;
    }
    public String getLanguage() {
        return language;
    }
    public void setLanguage(String language) {
        this.language = language;
    }
    public String getLicense() {
        return license;
    }

    public void setLicense(String license) {
        this.license = license;
    }
    public List<String> getDatasets() {
        return datasets;
    }
    public void setDatasets(List<String> datasets) {
        this.datasets = datasets;
    }

}
