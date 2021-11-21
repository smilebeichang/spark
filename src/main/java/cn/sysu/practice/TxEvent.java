package cn.sysu.practice;

/**
 * @Author : song bei chang
 * @create 2021/11/21 11:20
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}



