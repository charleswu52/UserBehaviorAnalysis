package networkflow_analysis.hotPages;

import networkflow_analysis.beans.PageViewCount;
import networkflow_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

/**
 * @author WuChao
 * @create 2021/6/23 14:46
 */
public class UniqueVisitor {
    /**
     * 网站独立访问数统计（UV）
     */
    public static void main(String[] args)throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据，创建DataStream
        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 开窗统计uv值
        SingleOutputStreamOperator<PageViewCount> uvCountStream = dataStream
                .filter(data -> "pv".equalsIgnoreCase(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UVCountWindow());

        uvCountStream.print();
        env.execute("uv count job");
    }

    public static class UVCountWindow implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            // 使用set保存全窗口中所有的userID
            HashSet<Long> userIdSet = new HashSet<>();
            for (UserBehavior ub : values) {
                userIdSet.add(ub.getUserId());
            }
            out.collect(new PageViewCount("uv",window.getEnd(), (long) userIdSet.size()));
        }
    }

}
