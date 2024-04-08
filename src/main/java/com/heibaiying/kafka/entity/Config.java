package com.heibaiying.kafka.entity;

import java.util.List;

public class Config {
    private List<String> architectures;
    private String model_type;

    // getters and setters
    public List<String> getArchitectures() {
        return architectures;
    }
    public void setArchitectures(List<String> architectures) {
        this.architectures = architectures;
    }
    public String getModel_type() {
        return model_type;
    }
    public void setModel_type(String model_type) {
        this.model_type = model_type;
    }

}
