package networkflow_analysis.hotPages;

import networkflow_analysis.beans.PageViewCount;
import networkflow_analysis.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;

/**
 * @author WuChao
 * @create 2021/6/23 15:37
 */

/**
 * @see  UniqueVisitor 一种改进，使用布隆过滤器保存全窗口中的userId  节省内存防止
 */
public class UniqueVIsitorWithBloomFilter {


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
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFliter());

        uvCountStream.print();


        env.execute("uv count with bloom filter job");
    }

    /**
     * 自定义 Trigger
     * 每来一个数据就触发一次计算
     */
    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每来一条数据就触发一次计算 并清空状态
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;  // 什么都不做
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;  // 什么都不做
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        }
    }

    /**
     * 实现自定义的 ProcessWindowFunction
     */

    public static class UvCountResultWithBloomFliter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        Jedis jedis;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            myBloomFilter = new MyBloomFilter(1 << 29); // 处理一亿次数据，大概使用 64MB 大小的位图
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
            // 将位图和窗口count值全部存入redis，用windowEnd作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();

            // 把count值存成一张 hash table
            String countHashTable = "uv_count";
            String countKey = windowEnd.toString();


            // 取当前的 userId
            Long userId = elements.iterator().next().getUserId();

            // 计算位图中的偏移量
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            // 用Redis的getbit 判断对应位置值
            Boolean isExist = jedis.getbit(bitmapKey, offset);

            if (!isExist) {
                // 对应位图部分置1
                jedis.setbit(bitmapKey, offset, true);

                // 更新Redis中保存的count值
                Long uvCount = 0L;  // 初始count值
                String uvCountString = jedis.hget(countHashTable, countKey);
                if (uvCountString != null && !"".equals(uvCountString)) {
                    uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(countHashTable, countKey, String.valueOf(uvCount + 1));

                out.collect(new PageViewCount("uv", windowEnd, uvCount + 1));
            }

        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }



    /**
     * 自定义  BloomFilter
     */
    public static class MyBloomFilter {
        // 定义位图的大小 一般为2的整数次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        // 实现自定义 hash 函数
        public Long hashCode(String value,Integer seed) {
            Long res = 0L;
            for (int i = 0; i < value.length(); i++) {
                res = res * seed + value.charAt(i);
            }
            return res & (cap - 1); // 取模运算
        }
    }

}
