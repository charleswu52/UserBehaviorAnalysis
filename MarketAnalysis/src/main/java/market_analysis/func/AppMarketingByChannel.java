package market_analysis.func;

/**
 * @author WuChao
 * @create 2021/6/23 20:00
 */

import market_analysis.beans.ChannelPromotionCount;
import market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * 根据不同渠道统计市场推广
 */
public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception {
        // 设置流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从自定义的数据源中获取数据
        DataStream<MarketingUserBehavior> dateStream = env.addSource(new SimulateMarketingBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                });

        // 分渠道开窗统计
        DataStream<ChannelPromotionCount> resultStream = dateStream
                .filter(data -> !"UNINSTALL".equalsIgnoreCase(data.getBehavior())) // 过滤卸载行为
                .keyBy("channel", "behavior") // 不同渠道下用户的不同行为为 ,使用组合key
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        resultStream.print();
        env.execute("app marketing by channel job");


    }

    /**
     * 自定义数据源
     * 模拟市场用户行为
     */
    public static class SimulateMarketingBehaviorSource implements SourceFunction<MarketingUserBehavior> {
        // 控制是否正常运行的标志位
        Boolean running = true;

        // 定义用户行为和来源渠道
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "wechat", "weibo");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (running) {
                // 随机产生 MarketingUserBehavior 的所有字段
                Long useId = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();

                // 生产数据
                ctx.collect(new MarketingUserBehavior(useId, behavior, channel, timestamp));

                Thread.sleep(100L); // 模拟数据产生间隙不要太快
            }

        }

        @Override
        public void cancel() {
            running = false;

        }
    }

    /**
     * 自定义增量聚合函数
     */
    public static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    /**
     * 自定义窗口函数
     */
    public static class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            Long count = elements.iterator().next();
            out.collect(new ChannelPromotionCount(channel, behavior, windowEnd, count));
        }
    }

}
