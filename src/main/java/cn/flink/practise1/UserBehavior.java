package cn.flink.practise1;

public class UserBehavior {
    public Long userId;
    public Long itemId;
    public Integer categoryId;
    public String behavior;
    public Long timestamp;

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp){
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public Long getItemId() {
        return itemId;
    }
}
