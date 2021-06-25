package orderpay_detect.func;

import orderpay_detect.beans.OrderEvent;
import orderpay_detect.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import scala.tools.nsc.doc.model.Val;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @author WuChao
 * @create 2021/6/25 16:07
 */
public class OrderPayTimeoutWithCEP {
    /**
     * 订单超时实时检测
     */
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从文件中读取数据
        URL resource = OrderPayTimeoutWithCEP.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env
                .readTextFile(resource.getPath())
                .map(data -> {
                    String[] fields = data.split(",");
                    return new OrderEvent(
                            new Long(fields[0]),
                            fields[1],
                            fields[2],
                            new Long(fields[3])
                    );
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 使用 CEP 来处理
        // 定义一个带时间限制的模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                })
                .followedBy("pay").where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "pay".equals(orderEvent.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 定义侧输出流标签，用来表示超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

        // 将pattern应用到输入数据流上，得到 pattern stream
        PatternStream<OrderEvent> patternStream = CEP
                .pattern(orderEventStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        // 调用select方法，实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream
                .select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect job");
    }

    /**
     * 实现自定义的超时事件处理函数
     */
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
            Long timeoutOrderId = map.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId, "timeout " + l);
        }
    }

    /**
     * 实现自定义的正常匹配事件处理函数
     */
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long payedOrderId = map.get("pay").iterator().next().getOrderId();
            return new OrderResult(payedOrderId, "payed");
        }
    }



}
