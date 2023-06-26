package cn.flink.practise2;



import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


// 自定义函数：TopN计算函数
public class TopNUrls extends KeyedProcessFunction<Long, UrlViewCountResult, String> {
    private final int topSize;

    public TopNUrls(int topSize) {
        this.topSize = topSize;
    }

    // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
    private ListState<UrlViewCountResult> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 状态的注册
        ListStateDescriptor<UrlViewCountResult> itemsStateDesc = new ListStateDescriptor<>("itemState-state", UrlViewCountResult.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void processElement(UrlViewCountResult input, Context context, Collector<String> collector) throws Exception {

        itemState.add(input);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        context.timerService().registerEventTimeTimer(input.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 获取收到的所有商品点击量
        List<UrlViewCountResult> allItems = new ArrayList<>();
        for (UrlViewCountResult item : itemState.get()) {
            allItems.add(item);
        }
        // 提前清除状态中的数据，释放空间
        itemState.clear();
        // 按照点击量从大到小排序
        allItems.sort(new Comparator<UrlViewCountResult>() {
            @Override
            public int compare(UrlViewCountResult o1, UrlViewCountResult o2) {
                return (int) (o2.getCount() - o1.getCount());
            }
        });
        // 将排名信息格式化成 String, 便于打印
        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < topSize; i++) {
            UrlViewCountResult currentItem = allItems.get(i);
            // No1:  商品ID=12224  浏览量=2413
            result.append("No").append(i).append(":")
                    .append("  url=").append(currentItem.getUrl())
                    .append("  浏览量=").append(currentItem.getCount())
                    .append("\n");
        }
        result.append("====================================\n\n");

        out.collect(result.toString());
    }
}