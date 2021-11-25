package cn.sysu.middlepractice;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * @Author : song bei chang
 * @create 2021/11/21 13:27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLog {
    private String ip;
    private long eventTime;
    private String method;
    private String url;
}


