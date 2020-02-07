package fefo.springframeworkftp.spring4ftpapp.services;

import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;
import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import fefo.springframeworkftp.spring4ftpapp.repository.ItemRepository;
import fefo.springframeworkftp.spring4ftpapp.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ItemServiceImpl implements ItemService {

    public ItemRepository itemRepository;

    @Autowired
    public ItemServiceImpl(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
    }

    @Override
    public List<ItemDetail> listAll() {
        List<ItemDetail> items = new ArrayList<>();
        itemRepository.findAll().forEach(items::add);
        return items;
    }

    @Override
    public ItemDetail getById(Long id) {
        return itemRepository.findById(id).orElse(null);
    }

    @Override
    public boolean isAvailable(Long id) {
        if(itemRepository.existsById(id))return true;
        return false;
    }

}
