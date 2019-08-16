package model.redis;


import java.io.Serializable;

public class OnWaitingSMSResponseRedisValue implements Serializable {
    private String msisdn;
    private String client_code;
    private String requestID;
    private long arrived_time;
    private long expire_time;

    @Override
    public String toString() {
        return "OnWaitingSMSResponseRedisValue{" +
                "msisdn='" + msisdn + '\'' +
                ", client_code='" + client_code + '\'' +
                ", requestID='" + requestID + '\'' +
                ", arrived_time=" + arrived_time +
                ", expire_time=" + expire_time +
                '}';
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public String getClient_code() {
        return client_code;
    }

    public void setClient_code(String client_code) {
        this.client_code = client_code;
    }

    public String getRequestID() {
        return requestID;
    }

    public void setRequestID(String requestID) {
        this.requestID = requestID;
    }

    public long getArrived_time() {
        return arrived_time;
    }

    public void setArrived_time(long arrived_time) {
        this.arrived_time = arrived_time;
    }

    public long getExpire_time() {
        return expire_time;
    }

    public OnWaitingSMSResponseRedisValue(String msisdn, String client_code, String requestID, long arrived_time, long expire_time) {

        this.msisdn = msisdn;
        this.client_code = client_code;
        this.requestID = requestID;
        this.arrived_time = arrived_time;
        this.expire_time = expire_time;
    }

    public void setExpire_time(long expire_time) {
        this.expire_time = expire_time;
    }
}
