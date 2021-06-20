package hotitems_analysis.hotItems;

import hotitems_analysis.beans.ItemViewCount;
import hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author WuChao
 * @create 2021/6/19 14:57
 */
public class HotItems {
    /**
     * 统计实时热门商品
     */

    public static void main(String[] args) throws Exception {

        // 创建流式执行环境 并配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从 文件 读取数据
        // DataStream<String> inputStream = env.readTextFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv");
        // 从 Kafka 读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "consumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("hotitems", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();    // 从头读取Kafka中的数据
        DataStream<String> inputStream = env.addSource(consumer);


        // 转化为POJO类型 分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
            // 日志数据已经经过了ETL清洗，按照时间戳升序排序
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L; // 时间戳毫秒数
            }
        });

        // 分组开窗聚合 得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) // 过滤 pv 行为
                .keyBy("itemId") // 根据 itemId 分组
                .timeWindow(Time.hours(1), Time.minutes(5)) // 开滑动窗口，1小时大小，5分钟滑动步长
                .aggregate(new ItemCountAgg(), new WidowItemCountResult());

        // 收集 同一窗口的所有商品的count数据，排序输出 topN
        DataStream<String> resultStream = windowAggStream
                .keyBy("windowEnd") // 按照窗口分组
                .process(new TopNHotItems(5)); // 取 Top N 热门商品

        resultStream.print("Top 5 HotItems");


        env.execute("hot items analysis");

    }

    /**
     * 实现自定义的增量聚合函数
     */
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            // 一般用不到
            return aLong + acc1;
        }
    }

    /**
     * 自定义全窗口函数
     * 取增量聚合的结果，即每个item对应的热度
     */
    public static class WidowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            // 获取包装成 ItemViewCount 的值
            Long itemId = tuple.getField(0);  // itemID
            Long windowEnd = window.getEnd(); // 当前窗口结束的时间
            Long count = input.iterator().next(); // count值
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    /**
     * 自定义的 KeyedProcessFunction
     */

    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        // TopN 的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义 ListState
            itemViewCountListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<ItemViewCount>("item-view-count", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            // 将 itemViewCountListState 中的数据转换成一个Arraylist进行排序
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(((o1, o2) -> o2.getCount().compareTo(o1.getCount())));

            // 将排名信息格式化成String，方便打印输出
            StringBuffer resultBuffer = new StringBuffer();
            resultBuffer.append("===================================\n");
            resultBuffer.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuffer.append("NO ").append(i + 1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuffer.append("===================================\n\n");

            // 控制输出频率，方便测试
            Thread.sleep(1000);

            out.collect(resultBuffer.toString());

        }
    }

}
