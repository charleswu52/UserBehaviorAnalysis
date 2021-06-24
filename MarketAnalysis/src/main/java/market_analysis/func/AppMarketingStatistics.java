package market_analysis.func;

/**
 * @author WuChao
 * @create 2021/6/23 20:43
 */

import market_analysis.beans.ChannelPromotionCount;
import market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 不分渠道统计推广数据量
 *
 * @see AppMarketingByChannel
 */
public class AppMarketingStatistics {
    public static void main(String[] args) throws Exception {
        // 设置流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从自定义的数据源中获取数据
        DataStream<MarketingUserBehavior> dateStream = env.addSource(new AppMarketingByChannel.SimulateMarketingBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                });

        // 开窗统计总量 不再区分渠道
        DataStream<ChannelPromotionCount> resultStream = dateStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        return new Tuple2<String, Long>("total", 1L);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketingStaticsticAgg(), new MarketingStatisticsResult());

        resultStream.print();

        env.execute("app marketing job");
    }

    /**
     * 自定义聚合函数
     */
    public static class MarketingStaticsticAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> stringLongTuple2, Long aLong) {
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
    public static class MarketingStatisticsResult implements WindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelPromotionCount> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new ChannelPromotionCount("total", "total", windowEnd, count));
        }
    }
}
