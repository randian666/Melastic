package com.handler.es.plugin.test;

import com.handler.es.plugin.annotation.JEAnalyzer;
import com.handler.es.plugin.annotation.MElasticColumn;
import com.handler.es.plugin.annotation.MElasticId;

import java.util.Date;

/**
 * Created by liuxun on 2017/2/28.
 */
public class User {
    @MElasticId
    private int id;
    @MElasticColumn(instore = true,analyzer = JEAnalyzer.analyzed)
    private String name;
    @MElasticColumn(instore = false,analyzer = JEAnalyzer.no)
    private Date postDate;
    @MElasticColumn(instore = true,analyzer = JEAnalyzer.analyzed)
    private String message;
    public User(){}
    public User(int id,String name,Date postDate,String message){
        this.id=id;
        this.name=name;
        this.postDate=postDate;
        this.message=message;
    }
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getPostDate() {
        return postDate;
    }

    public void setPostDate(Date postDate) {
        this.postDate = postDate;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
