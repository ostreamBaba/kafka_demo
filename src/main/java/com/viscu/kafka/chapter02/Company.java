package com.viscu.kafka.chapter02;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Builder;

/**
 * @author ostreamBaba
 * @date 2019/07/05 下午7:55
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Company {

    private String name;

    private String address;

}
