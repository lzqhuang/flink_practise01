package cn.flink.practise4;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * ## 需求四：订单支付实时监控
 * 最终创造收入和利润的是用户下单购买的环节；更具体一点，是用户真正完成支付动作的时候。
 * 用户下单的行为可以表明用户对商品的需求，但在现实中，并不是每次下单都会被用户立刻支付。
 * 当拖延一段时间后，用户支付的意愿会降低，并且为了降低安全风险，电商网站往往会对订单状态进行监控。
 * 设置一个失效时间（比如15分钟），如果下单后一段时间仍未支付，订单就会被取消。
 */
public class RealTimeMonitor {

    private static final FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        //todo: 1、构建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * dd1,1,2016-07-28 00:15:11,295
         * dd2,1,2016-07-28 00:16:12,165
         * dd2,2,2016-07-28 00:18:11,295
         * dd1,2,2016-07-28 00:18:12,165
         * dd2,3,2016-07-29 08:06:11,295
         * dd2,4,2016-07-29 12:21:12,165
         * dd3,1,2016-07-30 12:25:15,132
         * dd4,1,2016-07-30 12:30:24,165
         * dd5,1,2016-07-31 10:13:24,165
         * dd6,1,2016-07-31 12:13:24,165
         */
        //todo: 2、接受socket数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9999);

        //todo: 3、数据处理分组
        KeyedStream<OrderDetail, Tuple> keyedStream = socketTextStream.map(new MapFunction<String, OrderDetail>() {
            public OrderDetail map(String value) throws Exception {
                String[] strings = value.split(",");
                return new OrderDetail(strings[0], strings[1], strings[2], Double.parseDouble(strings[3]));
            }
            //添加watermark
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                //指定对应的TimestampAssigner
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        //指定EventTime对应的字段
                        long extractTimestamp = 0;
                        try {
                            extractTimestamp = fastDateFormat.parse(element.orderCreateTime).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return extractTimestamp;
                    }
                })).keyBy("orderId");

        //todo: 4、定义Parttern
        Pattern<OrderDetail, OrderDetail> pattern = Pattern.<OrderDetail>begin("start").where(new SimpleCondition<OrderDetail>() {
                    @Override
                    public boolean filter(OrderDetail orderDetail) throws Exception {

                        return orderDetail.status.equals("1");
                    }
                })
                .followedBy("second").where(new SimpleCondition<OrderDetail>() {
                    @Override
                    public boolean filter(OrderDetail orderDetail) throws Exception {

                        return orderDetail.status.equals("2");
                    }
                    //指定有效的时间约束
                }).within(Time.minutes(15));

        //todo: 5、将Parttern应用到事件流中进行检测，同时指定时间类型
        PatternStream<OrderDetail> patternStream = CEP.pattern(keyedStream, pattern).inEventTime();

        //todo: 6、提取匹配到的数据
        //定义侧输出流标签，存储超时的订单数据
        OutputTag outputTag = new OutputTag("timeout", TypeInformation.of(OrderDetail.class));

        SingleOutputStreamOperator<OrderDetail> result = patternStream.select(outputTag, new MyPatternTimeoutFunction(), new MyPatternSelectFunction());

        result.print("支付成功的订单：");

        result.getSideOutput(outputTag).print("超时的订单：");

        //todo: 7、提交任务
        env.execute("CheckOrderTimeoutWithCepByJava");

    }

    //todo: 提取正常支付成功的订单数据
    static class MyPatternSelectFunction implements PatternSelectFunction<OrderDetail, OrderDetail> {

        @Override
        public OrderDetail select(Map<String, List<OrderDetail>> patternMap) throws Exception {

            List<OrderDetail> secondOrderDetails = patternMap.get("second");
            //支付成功的订单
            OrderDetail orderDetailSuccess = secondOrderDetails.iterator().next();
            //返回
            return orderDetailSuccess;
        }
    }

    //todo: 提取超时的订单数据
    static class MyPatternTimeoutFunction implements PatternTimeoutFunction<OrderDetail, OrderDetail> {
        @Override
        public OrderDetail timeout(Map<String, List<OrderDetail>> patternMap, long timeoutTimestamp) throws Exception {

            List<OrderDetail> startTimeoutOrderDetails = patternMap.get("start");
            //超时订单
            OrderDetail orderDetailTimeout = startTimeoutOrderDetails.iterator().next();
            //返回
            return orderDetailTimeout;

        }
    }

    //todo:定义订单信息实体类
    public static class OrderDetail {
        //订单编号
        public String orderId;
        //订单状态
        public String status;
        //下单时间
        public String orderCreateTime;
        //订单金额
        public Double price;

        //无参构造必须带上
        public OrderDetail() {
        }

        public OrderDetail(String orderId, String status, String orderCreateTime, Double price) {
            this.orderId = orderId;
            this.status = status;
            this.orderCreateTime = orderCreateTime;
            this.price = price;
        }

        @Override
        public String toString() {
            return orderId + "\t" + status + "\t" + orderCreateTime + "\t" + price;
        }
    }
}