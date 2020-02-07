package fefo.springframeworkftp.spring4ftpapp.controllers;

import fefo.springframeworkftp.spring4ftpapp.repository.ItemRepository;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ItemController {

    private ItemRepository itemRepository;

    public ItemController(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
    }

    @RequestMapping("/items")
    public String getItems(Model model){

        model.addAttribute("items",itemRepository.findAll());

        return "items";
    }
}
