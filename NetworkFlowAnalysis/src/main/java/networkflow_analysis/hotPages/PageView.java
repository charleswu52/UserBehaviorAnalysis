package networkflow_analysis.hotPages;

import networkflow_analysis.beans.PageViewCount;
import networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;

/**
 * @author WuChao
 * @create 2021/6/21 21:37
 */
public class PageView {
    /**
     * 实时页面浏览量 (PV,Page View) 统计
     */
    public static void main(String[] args) throws Exception {

        // 创建流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从 文件 读取数据
        URL resource = PageView.class.getResource("/UserBehavior.csv");
        String resourcePath = resource.getPath();
        DataStream<String> inputStream = env.readTextFile(resourcePath);

        // 转化为POJO类型 分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(
                    new Long(fields[0]),
                    new Long(fields[1]),
                    new Integer(fields[2]),
                    fields[3],
                    new Long(fields[4]));
            // 日志数据已经经过了ETL清洗，按照时间戳升序排序
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L; // 时间戳毫秒数
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream0 = dataStream
                .filter(data -> "pv".equalsIgnoreCase(data.getBehavior())) // 过滤 PV 操作
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() { // 把所有数据 map 到一个二元组中
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(0) // 按第 0 列组，即所有数据都在一个桶里
                .timeWindow(Time.hours(1)) // 开一个小时的滚动窗口
                .sum(1); // 对count进行计数

//        pvResultStream0.print();

        // 并行任务改进，设计随机key，解决数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> pvWindowAggStream = dataStream
                .filter(data -> "pv".equalsIgnoreCase(data.getBehavior())) // 过滤 PV 操作
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        // 将数据随机分发到桶里
                        Random random = new Random();
                        return new Tuple2<Integer, Long>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountResult());

        //
        DataStream<PageViewCount> pvResultStream1 = pvWindowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new PvCountTotal());

        pvResultStream1.print();

        env.execute("page view count job");

    }

    /**
     * 自定义聚合函数
     */
    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> integerLongTuple2, Long aLong) {
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
     * 自定义全窗口函数
     */
    public static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()));
        }
    }

    /**
     * 自定义处理函数，把相同窗口分组统计的count值叠加
     */
    public static class PvCountTotal extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {
        // 定义状态，保存当前的总count值
        ValueState<Long> totalCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.totalCountState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Long>("total-count", Long.class, 0L));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            totalCountState.update(totalCountState.value() + value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
            Long totalCount = totalCountState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
            // 清空状态
            totalCountState.clear();
        }
    }
}
