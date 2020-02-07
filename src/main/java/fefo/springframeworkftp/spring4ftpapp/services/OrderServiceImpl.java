package fefo.springframeworkftp.spring4ftpapp.services;

import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import fefo.springframeworkftp.spring4ftpapp.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class OrderServiceImpl implements OrderService{

    private OrderRepository orderRepository;

    public OrderServiceImpl(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

  /*  @Override
    public List<OrderDetail> listAll() {
        List<OrderDetail> orders = new ArrayList<>();
        orderRepository.findAll().forEach(orders::add);
        return orders;
    }*/
    @Override
    public List listAll() {
        List orders = new ArrayList<>();
        orderRepository.findAll().forEach(orders::add);
        return orders;
    }

    @Override
    public OrderDetail getById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Invalid Order ID " + id));
    }

    @Override
    public boolean isAvailable(Long id) {
        if(orderRepository.existsById(id))return true;
        return false;
    }
}
