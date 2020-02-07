/*
package fefo.springframeworkftp.spring4ftpapp.batchWriters;

import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;
import fefo.springframeworkftp.spring4ftpapp.model.Order;
import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import fefo.springframeworkftp.spring4ftpapp.repository.ItemRepository;
import fefo.springframeworkftp.spring4ftpapp.repository.OrderRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class JpaOrderItemWriter implements ItemWriter<Order> {

    @Autowired
    OrderRepository orderRepository;
    @Autowired
    ItemRepository itemRepository;

    private String filePath;

    public JpaOrderItemWriter(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void write(List<? extends Order> list) throws Exception {
        for (Order order : list) {
            System.out.println("Items : " + order.toString());
            if (order instanceof OrderDetail) {
                ((OrderDetail) order).setBranchCode(filePath); //inserting the branchcode from the file name ex: BEY
                orderRepository.save((OrderDetail)order);
            }else if (order instanceof ItemDetail){

                OrderDetail orderDetail = new OrderDetail();
                orderDetail = orderRepository.findFirstByOrderByIdDesc();// get the latest order inserted in the DB,

                ItemDetail itemDetail = new ItemDetail();
                itemDetail = (ItemDetail) order;
                itemDetail.setBranchCode(filePath);

                itemDetail.getOrderDetails().add(orderRepository.findFirstByOrderByIdDesc()); //adding the link between items and their respective orders
                orderDetail.getItemDetails().add((ItemDetail)order);//adding the link between items and their respective orders

                itemRepository.save(itemDetail);
                orderRepository.save(orderDetail);
            }
        }
    }
}
*/
