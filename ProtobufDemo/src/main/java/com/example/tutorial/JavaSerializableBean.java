package com.example.tutorial;

import java.io.Serializable;

public class JavaSerializableBean implements Serializable {
    private String name;
    private String PhoneNo;
    private int id;
    private String email;

    public JavaSerializableBean(){

    }

    public JavaSerializableBean(String name, String phoneNo, int id, String email) {
        this.name = name;
        PhoneNo = phoneNo;
        this.id = id;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoneNo() {
        return PhoneNo;
    }

    public void setPhoneNo(String phoneNo) {
        PhoneNo = phoneNo;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
