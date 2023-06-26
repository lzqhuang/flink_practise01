package cn.flink.practise2;

// 自定义数据类型：日志事件
public class LoginEvent {
    public String ip;
    public String userId;
    public Long eventTime;
    public String method;
    public String url;

    // 对应的get和set方法，以及构造函数
    public LoginEvent(String ip, String userId, Long eventTime, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    public String getUserId() {
        return userId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public String getMethod() {
        return method;
    }

    public String getUrl() {
        return url;
    }
}
