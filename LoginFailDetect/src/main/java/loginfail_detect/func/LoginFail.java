package loginfail_detect.func;

/**
 * @author WuChao
 * @create 2021/6/24 16:19
 */

import loginfail_detect.beans.LoginEvent;
import loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 用户连续登录失败检测
 */
public class LoginFail {
    public static void main(String[] args)throws Exception {
        // 设置流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从文件中读取数据
        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 分配时间戳和WaterMark
        DataStream<LoginEvent> loginEventDataStream = inputStream
                .map(data -> {
                    String[] fields = data.split(",");
                    return new LoginEvent(
                            new Long(fields[0]),
                            fields[1],
                            fields[2],
                            new Long(fields[3])
                    );
                })
                .assignTimestampsAndWatermarks((new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        // 自定义处理函数检测连续登录失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventDataStream
                .keyBy(LoginEvent::getUseId)
                .process(new LoginFailDetectWarning(2));

        warningStream.print();

        env.execute("login fail detect job");
    }


    /*
     实现自定义KeyedProceeFunction 使用定时器
     */
    public static class LoginFailDetectWarning0 extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        // 最大连续登录失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning0(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }
        // 定义状态： 保存2秒内所有的登录失败事件
        ListState<LoginEvent> loginFailEventListState;
        // 定义状态： 保存注册的定时器时间戳
        ValueState<Long> timestampState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
            timestampState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-ts", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 判断当前事件登录状态
            if ("fail".equals(value.getLoginState())) {
                // 如果是登录失败事件，添加到列表状态中
                loginFailEventListState.add(value);
                // 如果没有定时器，就注册一个2秒的定时器
                if (timestampState.value() == null) {
                    Long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timestampState.update(ts);
                }
            } else {
                // 不是 登录失败 事件，删除定时器，清空状态，重新开始
                if (timestampState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(timestampState.value());
                }
                loginFailEventListState.clear();
                timestampState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            // 触发定时器，说明2秒内没有登录成功，判断ListState中失败的个数
            ArrayList<LoginEvent> loginEventArrayList = Lists.newArrayList(loginFailEventListState.get());
            Integer failTimes = loginEventArrayList.size();
            if (failTimes >= maxFailTimes) {
                // 超过最大失败次数，输出报警
                out.collect(new LoginFailWarning(
                        ctx.getCurrentKey(),
                        loginEventArrayList.get(0).getTimestamp(),
                        loginEventArrayList.get(loginEventArrayList.size() - 1).getTimestamp(),
                        "login fail in 2s for " + loginEventArrayList.size() + " times"
                ));
            }
            // 清空状态
            loginFailEventListState.clear();
            timestampState.clear();
        }
    }

    /*
     实现自定义KeyedProceeFunction 不使用定时器
     改进时效性，问题是默认按连续最大2次登录失败为例，代码扩展性不好
     */
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        // 最大连续登录失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 定义状态： 保存2秒内所有的登录失败事件
        ListState<LoginEvent> loginFailEventListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
        }



        /*
         以登录事件作为判断报警的触发条件，改进时效性，不再注册定时器
         */
        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            // 判断当前事件登录状态
            if ("fail".equals(value.getLoginState())) {
                // 如果是登录失败，获取状态中之前的登录失败事件，继续判断是否已有失败事件
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()) {
                    // 如果已经有登录失败事件，继续判断时间戳是否在2秒之内
                    // 获取已有的登录失败事件
                    LoginEvent firstLogFailEvent = iterator.next();
                    if (value.getTimestamp() - firstLogFailEvent.getTimestamp() <= 2) {
                        // 如果两次登录失败的时间在2秒以内 输出报警
                        out.collect(new LoginFailWarning(
                                value.getUseId(),
                                firstLogFailEvent.getTimestamp(),
                                value.getTimestamp(),
                                "login fail 2 times in 2s"
                        ));
                    }
                    // 不管报不报警，这次都已处理完毕，直接更新状态
                    loginFailEventListState.clear();
                    loginFailEventListState.add(value);
                } else {
                    //之前没有登录失败过 直接将当前事件存入ListState
                    loginFailEventListState.add(value);
                }
            } else {
                // 登录成功 清空状态
                loginFailEventListState.clear();
            }
        }
    }

}
