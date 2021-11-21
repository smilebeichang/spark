package cn.sysu.practice;

/**
 * @Author : song bei chang
 * @create 2021/11/21 11:18
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}


