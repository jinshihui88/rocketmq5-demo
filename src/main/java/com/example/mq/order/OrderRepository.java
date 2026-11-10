package com.example.mq.order;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 示例仓储：基于 JdbcTemplate + H2 内存库（零外部依赖，便于本地跑通）。
 */
@Repository
@RequiredArgsConstructor
public class OrderRepository {

    private final JdbcTemplate jdbcTemplate;

    public void save(OrderCreatedEvent event) {
        jdbcTemplate.update(
                "INSERT INTO order_info(order_no, user_id, amount, status) VALUES (?, ?, ?, 'CREATED')",
                event.orderNo(), event.userId(), event.amount());
    }

    public boolean existsByOrderNo(String orderNo) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM order_info WHERE order_no = ?",
                Integer.class, orderNo);
        return count != null && count > 0;
    }

    /**
     * 仅当订单仍为 CREATED（待支付）时关闭；返回是否真正执行了关单。
     */
    public boolean closeIfUnpaid(String orderNo) {
        int updated = jdbcTemplate.update(
                "UPDATE order_info SET status = 'CLOSED' WHERE order_no = ? AND status = 'CREATED'",
                orderNo);
        return updated > 0;
    }

    public String findStatus(String orderNo) {
        List<String> list = jdbcTemplate.query(
                "SELECT status FROM order_info WHERE order_no = ?",
                (rs, rowNum) -> rs.getString(1), orderNo);
        return list.isEmpty() ? null : list.get(0);
    }
}
