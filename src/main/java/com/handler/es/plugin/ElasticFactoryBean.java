package com.handler.es.plugin;

import com.handler.es.plugin.exception.MElasticRunTimeException;
import com.handler.es.plugin.melastic.MelasticClient;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.FactoryBean;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuxun on 2017/3/2.
 */
public class ElasticFactoryBean implements FactoryBean, Serializable {
    private String clusterName = "coupon-v2";
    private Boolean sniff=true;
    private String serverAddress="localhost:9300";
    private TransportClient client=null;
    private MelasticClient melasticClient=null;

    public TransportClient initClient() {
        Settings settings = Settings
                .settingsBuilder()
                .put("cluster.name",this.clusterName)//连接的集群名称
                //.put("client.transport.ping_timeout", clientPingTimeout)
                //.put("client.transport.nodes_sampler_interval", clientNodesSamplerInterval)
                .put("client.transport.sniff", this.sniff)//客户端允许“集群嗅探”
                .build();
        // 集群地址配置
        List<InetSocketTransportAddress> list = null;
        try {
            list = new ArrayList<InetSocketTransportAddress>();
            if (StringUtils.isNotEmpty(serverAddress)) {
                String[] addressStr = this.serverAddress.split(",");
                for (String s:addressStr){
                    String[] addressAndPort=s.split(":");
                    String address = addressAndPort[0];
                    int port = Integer.valueOf(addressAndPort[1]);
                    InetSocketTransportAddress inetSocketTransportAddress = new InetSocketTransportAddress(InetAddress.getByName(address), port);
                    list.add(inetSocketTransportAddress);
                }
            }
        } catch (Exception e) {
            throw new MElasticRunTimeException("init TransportClient error!"+e.getMessage());
        }
        // 这里可以同时连接集群的服务器,可以多个,并且连接服务是可访问的
        InetSocketTransportAddress addressList[] = list.toArray(new InetSocketTransportAddress[list.size()]);
        return TransportClient.builder().settings(settings).build().addTransportAddresses(addressList);
    }
    public Object getObject() throws Exception {
        if(StringUtils.isBlank(this.serverAddress)){
            throw new MElasticRunTimeException("init TransportAddress is null!");
        }
        if(this.serverAddress.indexOf(":") == -1){
            throw new MElasticRunTimeException("serverAddress param Format error!");
        }
        if(null == this.client){
            this.client = initClient();
            melasticClient=new MelasticClient(this.client);//工具类
        }
        return melasticClient;
    }

    public Class<?> getObjectType() {
        return ElasticFactoryBean.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setSniff(Boolean sniff) {
        this.sniff = sniff;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }
}
