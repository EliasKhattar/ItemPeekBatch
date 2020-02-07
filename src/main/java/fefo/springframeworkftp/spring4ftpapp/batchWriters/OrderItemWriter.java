package fefo.springframeworkftp.spring4ftpapp.batchWriters;

import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;
import fefo.springframeworkftp.spring4ftpapp.model.Order;
import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

public class OrderItemWriter implements ItemWriter<Order> {

    private static final String ORDER_DETAIL_INSERT_SQL = "INSERT INTO order_detail (ord, branch_code, eiclient_number, client_order_number, consigneep, payment_terms, earliest_ship_date, latest_ship_date, consignee_code, bill_to_code, third_party_bill_to_customer_no, preferred_carrier, usd1, usd2)" +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String ITEM_DETAIL_INSERT_SQL = "INSERT INTO item_detail (ord_no, branch_code, detail, line_number, sku_number, qty, lot_no, receipt_po, usd1, usd2, usd3, usd4, usd5)" +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
    private JdbcTemplate jdbcTemplate;

    public OrderItemWriter(DataSource datasource) {
        this.jdbcTemplate = new JdbcTemplate(datasource);
        //this.filePath = filePath;
    }
    @Override
    public void write(List<? extends Order> list) throws Exception {
        for (Order order : list){
            if(order instanceof OrderDetail){

                jdbcTemplate.update(ORDER_DETAIL_INSERT_SQL,
                        ((OrderDetail) order).getOrd(),
                        "BEY",
                         /*branchCode,*/
                        ((OrderDetail) order).getEiclientNumber(),
                        ((OrderDetail) order).getClientOrderNumber(),
                        ((OrderDetail) order).getConsigneeP(),
                        ((OrderDetail) order).getPaymentTerms(),
                        ((OrderDetail) order).getEarliestShipDate(),
                        ((OrderDetail) order).getLatestShipDate(),
                        ((OrderDetail) order).getConsigneeCode(),
                        ((OrderDetail) order).getBillToCode(),
                        ((OrderDetail) order).getThirdPartyBillToCustomerNo(),
                        ((OrderDetail) order).getPreferredCarrier(),
                        ((OrderDetail) order).getUsd1(),
                        ((OrderDetail) order).getUsd2()
                );
            /*}else if(order instanceof ItemDetail){
                jdbcTemplate.update(ITEM_DETAIL_INSERT_SQL, (
                                //(ItemDetail) order).getOrdNo(),
                        //"BEY",
                        ((ItemDetail) order).getDetail(),
                        ((ItemDetail) order).getLineNumber(),
                        ((ItemDetail) order).getSkuNumber(),
                        ((ItemDetail) order).getQty(),
                        ((ItemDetail) order).getLotNo(),
                        ((ItemDetail) order).getReceiptPo(),
                        ((ItemDetail) order).getUsd1(),
                        ((ItemDetail) order).getUsd2(),
                        ((ItemDetail) order).getUsd3(),
                        ((ItemDetail) order).getUsd4()
                );*/
            }
        }

    }
}
