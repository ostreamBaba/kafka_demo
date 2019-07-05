package com.viscu.kafka.chapter02;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * @author ostreamBaba
 * @date 2019/07/05 下午7:32
 */

// 自定义字符串序列化

@Slf4j
public class StringSerializer implements Serializer<String>{

    public static String encoding = "utf8";

    // 设置字符串的编码格式 一般默认utf8
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if(encodingValue == null){
            encodingValue = configs.get("serializer.encoding");
        }
        if(encodingValue != null && encodingValue instanceof String){
            encoding = (String) encodingValue;
        }
    }

    @Override
    public byte[] serialize(String topic, String data) {
        if(StringUtils.isEmpty(data)){
            return null;
        }
        try {
            return data.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            log.error(this.getClass().getName() + ":" + e.getMessage());
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
