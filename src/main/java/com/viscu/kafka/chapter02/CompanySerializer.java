package com.viscu.kafka.chapter02;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author ostreamBaba
 * @date 2019/07/05 下午7:55
 */

@Slf4j
public class CompanySerializer implements Serializer<Company>{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    // 设置company的序列化工具
    @Override
    public byte[] serialize(String topic, Company data) {
        if(data == null){
            return null;
        }
        byte[] name, address;
        try {
            if (StringUtils.isEmpty(data.getName())){
                name = data.getName().getBytes("utf-8");
            } else {
                name = new byte[0];
            }
            if(StringUtils.isEmpty(data.getAddress())){
                address = data.getAddress().getBytes("utf-8");
            } else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            log.error(this.getClass().getName() + ":" + e.getMessage());
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
