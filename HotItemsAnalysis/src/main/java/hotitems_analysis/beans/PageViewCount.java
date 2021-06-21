package hotitems_analysis.beans;

/**
 * @author WuChao
 * @create 2021/6/21 22:11
 */

/**
 * PV Bean 每个页面的浏览量
 */
public class PageViewCount {
    private String url;
    private Long windowEnd;
    private Long count;

    public PageViewCount() {
    }

    public PageViewCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PageViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
