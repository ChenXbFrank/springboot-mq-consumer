package com.cxb.mq.consumer;

import com.cxb.mq.entity.Order;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class OrderReceiver {

    // 指定监听消息的队列
    @RabbitListener(bindings = @QueueBinding(
                                    value = @Queue(value = "order-queue",
                                    durable = "true"),
                    exchange = @Exchange(
                            name = "order-exchange",
                            durable = "true",
                            type = "topic"),
                    key = "order.#"
    ))

    /**
     *  这里我们知道是发送的订单消息
     * @param order
     * @param headers
     * @param channel
     * @throws Exception
     */
    @RabbitHandler
    public void onOrderMessage(@Payload Order order,
                               @Headers Map<String, Object> headers,
                               Channel channel) throws Exception{
        System.out.println("接收消息，开始消费....");
        System.out.println("订单ID：" + order.getId());
        // 手动确认接收完成，告诉mq已经消费完成了
        Long deliveryTag = (Long) headers.get(AmqpHeaders.DELIVERY_TAG);
        channel.basicAck(deliveryTag, false);
    }
}
