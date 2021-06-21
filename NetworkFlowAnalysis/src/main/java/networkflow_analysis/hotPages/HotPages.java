package networkflow_analysis.hotPages;

import networkflow_analysis.beans.ApacheLogEvent;
import networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.smartcardio.CardTerminal;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author WuChao
 * @create 2021/6/20 20:51
 */
public class HotPages {

    /**
     * 实时热门页面统计
     */

    public static void main(String[] args) throws Exception {
        // 定义 Flink 流式执行环境，并配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取文件 转换为 POJO
        URL resource = HotPages.class.getResource("/apache.log");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("DD/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getTimestamp();
            }
        });

        // 定义侧输出流标签 收集 漏掉的数据
        OutputTag<ApacheLogEvent> late_data = new OutputTag<ApacheLogEvent>("late data") {
        };

        // 分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(event -> "GET".equalsIgnoreCase(event.getMethod())) // 过滤 GET 请求
                .filter(event -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, event.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)  // 按照 url 分组
                .timeWindow(Time.minutes(10), Time.seconds(5)) // 滑动窗口
                .allowedLateness(Time.minutes(1)) // 允许乱序数据迟到 1 分钟
                .sideOutputLateData(late_data)  // 迟到数据 送往 侧输出流
                .aggregate(new PageCountAgg(), new PageCountResult());

        // 输出 侧输出流中的数据
        windowAggStream.print("agg");
        windowAggStream.getSideOutput(late_data).print("late data");

        // 收集同一窗口的count数据，排序输出
        DataStream<String> resultStream = windowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        resultStream.print();

        // 执行
        env.execute("network flow analysis");
    }

    /**
     * 增量聚合函数
     * PageCountAgg  页面的聚合函数
     */
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
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
     * 全窗口函数
     * 增量聚合的结果 每个页面 URL 对应的访问次数
     */
    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            // 包装成 PageViewCount 输出
            out.collect(new PageViewCount(s, window.getEnd(), input.iterator().next()));
        }
    }

    /**
     * 自定义的 KeyedProcessFunction
     */
    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        // TopN 的大小
        private int topSize;

        public TopNHotPages(int topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的 PageViewCount
//        ListState<PageViewCount> pageViewCountListState;
        // 改良 => 定义列表状态，保存当前窗口内所有输出的 PageViewCount 到 Map 保证乱序数据
        MapState<String, Long> pageViewCountMapState;


        @Override
        public void open(Configuration parameters) throws Exception {
//            this.pageViewCountListState = getRuntimeContext()
//                    .getListState(new ListStateDescriptor<PageViewCount>("page-view-count", PageViewCount.class));
            this.pageViewCountMapState = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<>("page-view-count", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入ListState中，并注册定时器
//            pageViewCountListState.add(value);
            pageViewCountMapState.put(value.getUrl(), value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

            // 注册一个 1分钟后的定时器用来清空状态
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出

            // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                pageViewCountMapState.clear();
                return;
            }

            // 将 itemViewCountMapState 中的数据转换成一个Arraylist进行排序
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists
                    .newArrayList(pageViewCountMapState.entries().iterator());

            // 排序
            pageViewCounts.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

            // 将排名信息格式化成String，方便打印输出
            StringBuffer resultBuffer = new StringBuffer();
            resultBuffer.append("===================================\n");
            resultBuffer.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentPageViewCount = pageViewCounts.get(i);
                resultBuffer.append("NO ").append(i + 1).append(":")
                        .append(" URL = ").append(currentPageViewCount.getKey())
                        .append(" 热门度 = ").append(currentPageViewCount.getValue())
                        .append("\n");
            }
            resultBuffer.append("===================================\n\n");
            Thread.sleep(1000);

            out.collect(resultBuffer.toString());


        }
    }
}
