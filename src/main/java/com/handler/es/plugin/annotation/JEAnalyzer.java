package com.handler.es.plugin.annotation;

/**
 * Created by liuxun on 2017/3/2.
 */
public enum JEAnalyzer {
    /**
      no: 不把此字段添加到索引中，也就是不建索引，此字段不可查询
      not_analyzed:将字段的原始值放入索引中，作为一个独立的term，它是除string字段以外的所有字段的默认值。
      analyzed:string字段的默认值，会先进行分析后，再把分析的term结果存入索引中。
     */
    analyzed("analyzed"),not_analyzed("not_analyzed"), no("no");

    private String value;

    private JEAnalyzer(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }
}
