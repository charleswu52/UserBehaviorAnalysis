package orderpay_detect.func;

import orderpay_detect.beans.OrderEvent;
import orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @author WuChao
 * @create 2021/6/26 14:32
 */
public class TxPayMatch {
    /**
     * 实现来自两条流的订单交易匹配
     * 使用Connect连接的方式
     */

    // 定义侧输出流
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays"){};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts"){};
    public static void main(String[] args)throws Exception {
        // 设置流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据并转换成POJO类型

        // 读取订单支付事件数据
        URL orderResource = TxPayMatch.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env
                .readTextFile(orderResource.getPath())
                .map( line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                } )
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                })
                .filter( data -> !"".equals(data.getTxId()) );    // 交易id不为空，必须是pay事件

        // 读取到账事件数据
        URL receiptResource = TxPayMatch.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env
                .readTextFile(receiptResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 对获取的两条数据流进行合并，匹配处理，不匹配的事件会输出到侧输出流
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect());

        resultStream.print("matched-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");

        env.execute("tx match detect job");

    }

    /**
     * 实现自定义的 CoProcessFunction
     */
    public static class TxPayMatchDetect extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
        // 定义状态，保存当前已经到来的订单支付事件和到账时间
        ValueState<OrderEvent> orderPayEventValueState;
        ValueState<ReceiptEvent> receiptEventValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.orderPayEventValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<OrderEvent>("pay", OrderEvent.class));
            this.receiptEventValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<ReceiptEvent>("receipt", ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 处理元素1，即订单支付事件到来，判断是否有到账事件
            ReceiptEvent receipt = receiptEventValueState.value();
            if (receipt != null) {
                // 不为空说明到账事件已经来过，输出匹配信息，
                out.collect(new Tuple2<>(value, receipt));
                // 清空状态
                orderPayEventValueState.clear();
                receiptEventValueState.clear();
            } else {
                // 到账事件还没来，就注册一个定时器等待
                ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 5) * 1000L); // 等待5秒，具体看数据
                // 更新状态
                orderPayEventValueState.update(value);
            }

        }

        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 到账事件到来，判断是否有对应的支付事件
            OrderEvent orderPay = orderPayEventValueState.value();
            if (orderPay != null) {
                // orderPay 不为空说明有支付事件
                out.collect(new Tuple2<>(orderPay, value));
                // 清空状态
                orderPayEventValueState.clear();
                receiptEventValueState.clear();
            } else {
                // 没有订单支付事件到来，也注册一个定时器等待
                ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 3) * 1000L); // 等待3秒，具体看数据
                // 更新状态
                receiptEventValueState.update(value);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 定时器触发，前面定义了两种计时器要进行判断是否有一个事件没有到来
            if (orderPayEventValueState.value() != null) {
                // 说明 到账事件没来
                ctx.output(unmatchedPays, orderPayEventValueState.value());
            }
            if (receiptEventValueState.value() != null) {
                // 说明 支付订单事件没来
                ctx.output(unmatchedReceipts, receiptEventValueState.value());
            }
            // 清空状态
            orderPayEventValueState.clear();
            receiptEventValueState.clear();
        }
    }



}
