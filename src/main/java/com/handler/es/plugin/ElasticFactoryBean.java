package com.handler.es.plugin;

import com.handler.es.plugin.exception.MElasticRunTimeException;
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
    private String schedule="50ms";
    private Boolean autoCreateIndex=false;
    private String gatewayType="local";
    private String initialStateTimeout="500ms";
    private String pingTimeout="200ms";
    private Integer minimumMasterNodes=2;
    private String discoveryType="zen";
    private String serverAddress="localhost:9300";
    private Integer indexNumberOfShards=1;
    private Integer numberOfShards=5;
    private TransportClient client=null;

    public TransportClient initClient() {
        Settings settings = Settings
                .settingsBuilder()
                .put("discovery.type", this.discoveryType) //发现集群方式
                .put("discovery.zen.minimum_master_nodes", this.minimumMasterNodes) //最少有2个master存在
                .put("discovery.zen.ping_timeout", this.pingTimeout) //集群ping时间，太小可能会因为网络通信而导致不能发现集群
                .put("discovery.initial_state_timeout", this.initialStateTimeout)
                .put("gateway.type", this.gatewayType)
                .put("action.auto_create_index", this.autoCreateIndex)//-配置是否自动创建索引（true OR false);
                .put("cluster.routing.schedule", this.schedule)//如（50ms)， 发现新节点时间
                .put("index.number_of_shards", this.indexNumberOfShards)//设置一个索引可被复制的数量,默认值为1
//                .put("number_of_shards",this.numberOfShards)//设置主分片默认是5
//                .put("number_of_replicas", 1)//测试环境，减少副本提高速度
                .put("cluster.name",this.clusterName)//连接的集群名称
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
        }
        return client;
    }

    public Class<?> getObjectType() {
        return ElasticFactoryBean.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void setNumberOfShards(Integer numberOfShards) {
        this.numberOfShards = numberOfShards;
    }

    public void setIndexNumberOfShards(Integer indexNumberOfShards) {
        this.indexNumberOfShards = indexNumberOfShards;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setSniff(Boolean sniff) {
        this.sniff = sniff;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public void setAutoCreateIndex(Boolean autoCreateIndex) {
        this.autoCreateIndex = autoCreateIndex;
    }

    public void setGatewayType(String gatewayType) {
        this.gatewayType = gatewayType;
    }

    public void setInitialStateTimeout(String initialStateTimeout) {
        this.initialStateTimeout = initialStateTimeout;
    }

    public void setPingTimeout(String pingTimeout) {
        this.pingTimeout = pingTimeout;
    }

    public void setMinimumMasterNodes(Integer minimumMasterNodes) {
        this.minimumMasterNodes = minimumMasterNodes;
    }

    public void setDiscoveryType(String discoveryType) {
        this.discoveryType = discoveryType;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }
}
