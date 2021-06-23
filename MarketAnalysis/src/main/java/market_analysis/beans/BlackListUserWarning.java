package market_analysis.beans;

/**
 * @author WuChao
 * @create 2021/6/23 23:31
 */

/**
 * 用户点击的黑名单
 * 用户对某个广告在一段时间内疯狂点击 拉黑名单并记录报警信息
 */
public class BlackListUserWarning {
    private Long userId;
    private Long adId;
    private String warningMsg;

    public BlackListUserWarning() {
    }

    public BlackListUserWarning(Long userId, Long adId, String warningMsg) {
        this.userId = userId;
        this.adId = adId;
        this.warningMsg = warningMsg;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "BlackListUserWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }
}
