package com.example.mq.web;

import com.example.mq.order.OrderCreatedEvent;
import com.example.mq.order.OrderRepository;
import com.example.mq.order.OrderService;
import com.example.mq.order.OrderTxService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

/**
 * 演示接口：通过 HTTP 触发各类消息的发送，便于联调验证。
 */
@RestController
@RequestMapping("/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final OrderTxService orderTxService;
    private final OrderRepository orderRepository;

    /** 普通消息：订单创建通知 */
    @PostMapping("/normal")
    public Map<String, Object> normal(@RequestParam String orderNo,
                                      @RequestParam(defaultValue = "1") Long userId,
                                      @RequestParam(defaultValue = "99.50") BigDecimal amount) {
        orderService.notifyCreated(new OrderCreatedEvent(orderNo, userId, amount));
        return Map.of("result", "normal message sent", "orderNo", orderNo);
    }

    /** 异步发送普通消息 */
    @PostMapping("/async")
    public Map<String, Object> async(@RequestParam String orderNo,
                                     @RequestParam(defaultValue = "1") Long userId,
                                     @RequestParam(defaultValue = "99.50") BigDecimal amount) {
        orderService.notifyCreatedAsync(new OrderCreatedEvent(orderNo, userId, amount));
        return Map.of("result", "async message sent", "orderNo", orderNo);
    }

    /** 顺序消息：同一订单连续发送 CREATED -> PAID -> SHIPPED 三条状态流转 */
    @PostMapping("/fifo")
    public Map<String, Object> fifo(@RequestParam String orderNo) {
        orderService.publishStatus(orderNo);
        return Map.of("result", "3 fifo messages sent (CREATED/PAID/SHIPPED)", "orderNo", orderNo);
    }

    /** 延迟消息：超时关单，delaySeconds 默认 10 秒便于演示 */
    @PostMapping("/delay")
    public Map<String, Object> delay(@RequestParam String orderNo,
                                     @RequestParam(defaultValue = "10") long delaySeconds) {
        orderService.scheduleClose(new OrderCreatedEvent(orderNo, 1L, new BigDecimal("99.50")),
                Duration.ofSeconds(delaySeconds));
        return Map.of("result", "delay message sent", "orderNo", orderNo, "delaySeconds", delaySeconds);
    }

    /** 事务消息：下单（半消息 + 本地事务 + 提交，库存服务异步消费） */
    @PostMapping("/tx")
    public Map<String, Object> tx(@RequestParam String orderNo,
                                  @RequestParam(defaultValue = "1") Long userId,
                                  @RequestParam(defaultValue = "99.50") BigDecimal amount) throws Exception {
        orderTxService.createOrder(new OrderCreatedEvent(orderNo, userId, amount));
        return Map.of("result", "transaction message committed", "orderNo", orderNo);
    }

    /** 查询订单状态（验证事务消息落库与超时关单效果） */
    @GetMapping("/{orderNo}")
    public Map<String, Object> status(@PathVariable String orderNo) {
        String status = orderRepository.findStatus(orderNo);
        return status == null
                ? Map.of("orderNo", orderNo, "status", "NOT_EXIST")
                : Map.of("orderNo", orderNo, "status", status);
    }
}
