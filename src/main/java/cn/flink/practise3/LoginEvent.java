package cn.flink.practise3;

public class LoginEvent {
    public String ip;
    public String userId;
    public Long eventTime;
    public String result;


    // 对应的get和set方法，以及构造函数
    public LoginEvent(String ip, String userId, Long eventTime, String result) {
        this.ip = ip;
        this.userId = userId;
        this.eventTime = eventTime;
        this.result = result;
    }

    public String getUserId() {
        return userId;
    }
}
