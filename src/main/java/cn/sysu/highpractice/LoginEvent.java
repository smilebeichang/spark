package cn.sysu.highpractice;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : song bei chang
 * @create 2021/11/21 13:40
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;
}


