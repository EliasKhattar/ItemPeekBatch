package fefo.springframeworkftp.spring4ftpapp.repository;

import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;
import org.springframework.data.repository.CrudRepository;

public interface ItemRepository extends CrudRepository<ItemDetail,Long> {
}
