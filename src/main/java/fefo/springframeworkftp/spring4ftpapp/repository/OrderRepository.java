package fefo.springframeworkftp.spring4ftpapp.repository;

import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import org.springframework.data.repository.CrudRepository;

public interface OrderRepository extends CrudRepository<OrderDetail,Long> {

     OrderDetail findFirstByOrderByIdDesc();
}
