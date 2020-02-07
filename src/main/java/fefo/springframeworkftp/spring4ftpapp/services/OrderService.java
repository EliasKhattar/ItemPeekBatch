package fefo.springframeworkftp.spring4ftpapp.services;

import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;

import java.util.List;

public interface OrderService {

    List<OrderDetail> listAll();

    OrderDetail getById(Long id);

    boolean isAvailable(Long id);
}
