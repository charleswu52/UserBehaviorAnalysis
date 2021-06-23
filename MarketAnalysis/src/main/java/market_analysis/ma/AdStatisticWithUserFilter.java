package market_analysis.ma;

/**
 * @author WuChao
 * @create 2021/6/23 23:09
 */

import market_analysis.beans.AdClickEvent;
import market_analysis.beans.AdCountViewByProvince;
import market_analysis.beans.BlackListUserWarning;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * 根据不同省份用户点击行为进行同时对用户“刷单”行为进行过滤
 *
 */
public class AdStatisticWithUserFilter {
    public static void main(String[] args)throws Exception {
        // 设置流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从文件中读取数据
        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 转换数据并分配时间戳和WaterMark
        DataStream<AdClickEvent> adClickStream = inputStream
                .map(data -> {
                    String[] fields = data.split(",");
                    return new AdClickEvent(
                            new Long(fields[0]),
                            new Long(fields[1]),
                            fields[2],
                            fields[3],
                            new Long(fields[4])
                    );
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //  对同一个用户点击同一个广告的行为进行检测报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickStream
                .keyBy("userId", "adId")    // 基于用户id和广告id做分组
                .process(new FilterBlackListUser(100)); // 根据点击次数判断是否报警

        // 基于省份分组，开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = filterAdClickStream
                .keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))     // 定义滑窗，5分钟输出一次
                .aggregate(new AdStatisticsByProvince.AdCountAgg(), new AdStatisticsByProvince.AdCountResult());

        adCountResultStream.print();
        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist"){}).print("blacklist-user");

        env.execute("ad count by province job");
    }

    /**
     * 实现自定义处理函数
     */
    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {
        // 定义属性，点击次数上限
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        // 定义状态，保存当前用户对某一广告的点击次数
        ValueState<Long> countState;
        // 定义一个标志状态，保存当前用户是否已经被发送到了黑名单里
        ValueState<Boolean> isSentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count", Long.class, 0L));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class, false));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            // 判断当前用户对同一广告的点击次数，如果不够上限，就count加1正常输出；如果达到上限，直接过滤掉，并侧输出流输出黑名单报警
            // 首先获取当前的count值
            Long curCount = countState.value();

            // 判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
            if (curCount == 0) {
                Long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1)
                        * (24 * 60 * 60 * 1000)
                        - 8 * 60 * 60 * 1000;
                ctx.timerService().registerEventTimeTimer(ts);
            }

            // 判断是否报警
            if (curCount >= countUpperBound) {
                // 判断是否输出到黑名单过，如果没有的话就输出到侧输出流
                if (!isSentState.value()) {
                    isSentState.update(true); // 更新状态
                    ctx.output(new OutputTag<BlackListUserWarning>("blacklist") {
                               },
                            new BlackListUserWarning(
                                    value.getUserId(),
                                    value.getAdId(),
                                    "click over " + countUpperBound + "times."));
                }
                return;
            }
            // 如果没有返回 点击次数加1，更新状态，正常输出当前数据到主流中
            countState.update(curCount + 1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            // 清空所有状态
            countState.clear();
            isSentState.clear();
        }
    }

}
