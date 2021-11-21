package cn.sysu.highpractice;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : song bei chang
 * @create 2021/11/21 13:34
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class AdsClickLog {
    private long userId;
    private long adsId;
    private String province;
    private String city;
    private Long timestamp;

}


