package fefo.springframeworkftp.spring4ftpapp.services;

import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;

import java.util.List;

public interface ItemService {

    List<ItemDetail> listAll();

    ItemDetail getById(Long id);

    boolean isAvailable(Long id);
}
