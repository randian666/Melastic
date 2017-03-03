package com.handler.es.plugin.exception;

/**
 * Created by liuxun on 2017/3/2.
 */
public class MElasticRunTimeException extends RuntimeException{
    private static final long serialVersionUID = -1672618577869358450L;
    public MElasticRunTimeException(Exception e) {
        super(e);
    }

    public MElasticRunTimeException(Exception e, String errorMsg) {
        super(errorMsg, e);
    }

    public MElasticRunTimeException(String errorMsg) {
        super(errorMsg);
    }
}
