package cn.sysu.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : song bei chang
 * @create 2021/4/18 21:38
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {

    /**
     * 传感器编号
     */
    private String id;

    /**
     * 时间戳
     */
    private Long ts;

    /**
     * 水位
     */
    private Integer vc;


}



