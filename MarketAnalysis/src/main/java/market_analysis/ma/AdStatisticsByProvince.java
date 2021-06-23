package market_analysis.ma;

/**
 * @author WuChao
 * @create 2021/6/23 21:08
 */

import market_analysis.beans.AdClickEvent;
import market_analysis.beans.AdCountViewByProvince;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;

/**
 * 根据不同省份 统计广告点击数据
 */
public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception {
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

        // 基于省份分组，开窗聚合
        DataStream<AdCountViewByProvince> adCountResultStream = adClickStream
                .keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();

        env.execute("ad count by province job");

    }

    /**
     * 自定义的聚合函数
     */
    public static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long aLong) {
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
    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
            out.collect(new AdCountViewByProvince(s,new Timestamp(window.getEnd()).toString(),input.iterator().next()));
        }
    }

}
