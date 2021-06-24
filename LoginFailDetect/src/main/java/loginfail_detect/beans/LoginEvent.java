package loginfail_detect.beans;

/**
 * @author WuChao
 * @create 2021/6/24 16:09
 */

/**
 * 用户登录事件的POJO类
 */
public class LoginEvent {
    private Long useId;
    private String ip;
    private String loginState;
    private Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(Long useId, String ip, String loginState, Long timestamp) {
        this.useId = useId;
        this.ip = ip;
        this.loginState = loginState;
        this.timestamp = timestamp;
    }

    public Long getUseId() {
        return useId;
    }

    public void setUseId(Long useId) {
        this.useId = useId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getLoginState() {
        return loginState;
    }

    public void setLoginState(String loginState) {
        this.loginState = loginState;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "useId=" + useId +
                ", ip='" + ip + '\'' +
                ", loginState='" + loginState + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
