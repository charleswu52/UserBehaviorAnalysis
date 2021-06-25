package orderpay_detect.func;

import orderpay_detect.beans.OrderEvent;
import orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.PreDestroy;
import java.net.URL;

/**
 * @author WuChao
 * @create 2021/6/25 16:07
 */
public class OrderPayTimeoutWithoutCEP {
    /**
     * 订单超时实时检测
     */

    // 定义超时事件的侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};


    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从文件中读取数据
        URL resource = OrderPayTimeoutWithoutCEP.class.getResource("/OrderLog.csv");
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

        // 不使用 CEP 来处理
        // 自定义处理函数，主流输出正常匹配订单事件，侧输出流输出超时报警事件
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());


        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without cep job");


    }

    /**
     * 自定义处理函数
     */
    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {
        // 定义状态，保存之前订单是否已经来过create、pay的事件
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreatedState;
        // 定义状态，保存定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isCreatedState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("is-created", Boolean.class,false)
            );
            isPayedState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false)
            );
            timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("time-ts", Long.class, 0L)
            );
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            // 获取当前状态
            Boolean isPayed = isPayedState.value();
            Boolean isCreated = isCreatedState.value();
            Long timeTs = timerTsState.value();

            // 判断当前事件类型
            if ("create".equals(value.getEventType())) {
                if (isPayed) {
                    // 如果已经正常支付，正常支付
                    out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    // 清空状态 删除定时器
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timeTs);

                } else {
                    // 没有支付过就注册一个15分钟后的定时器，等待支付
                    Long ts = (value.getTimestamp() + 15 * 60) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    // 更新状态
                    timerTsState.update(ts);
                    isCreatedState.update(true);
                }
            } else if ("pay".equals(value.getEventType())) {
                // 当前是一个支付状态，判断是否有下单事件
                if (isCreated) {
                    // 已经有过下单事件，要继续判断支付的时间戳是否超过15分钟
                    if (value.getTimestamp() * 1000L < timeTs) {
                        // 在15分钟内，说明没有超时，正常输出
                        out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    } else {
                        ctx.output(orderTimeoutTag,new OrderResult(value.getOrderId(), "payed but already timeout"));
                    }
                    // 统一清空状态
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timeTs);
                } else {
                    // 没有下过单，说明是乱序到达的，也是注册一个定时器，等待下单事件
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L);
                    // 更新状态
                    timerTsState.update(value.getTimestamp() * 1000L);
                    isPayedState.update(true);
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定时器触发，说明一定有一个事件没来
            if (isPayedState.value()) {
                // 如果pay来了，说明create没来
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not found created log"));
            } else {
                // 如果pay没来，支付超时
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "timeout"));
            }
            // 清空状态
            isCreatedState.clear();
            isPayedState.clear();
            timerTsState.clear();
        }
    }
}

