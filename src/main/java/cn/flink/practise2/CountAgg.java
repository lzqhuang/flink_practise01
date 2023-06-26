package cn.flink.practise2;



import org.apache.flink.api.common.functions.AggregateFunction;

// 自定义函数：计数器
public class CountAgg implements AggregateFunction<LoginEvent, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(LoginEvent value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return null;
    }
}
