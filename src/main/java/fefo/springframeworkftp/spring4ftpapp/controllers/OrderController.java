package fefo.springframeworkftp.spring4ftpapp.controllers;

import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import fefo.springframeworkftp.spring4ftpapp.services.ItemService;
import fefo.springframeworkftp.spring4ftpapp.services.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Controller
public class OrderController {

    private OrderService orderService;
    private ItemService itemService;


    public OrderController(OrderService orderService, ItemService itemService) {
        this.orderService = orderService;
        this.itemService = itemService;
    }

    @RequestMapping("orders/orders")
    public String listOrders(Model model){

        List<OrderDetail>  orderDetails = new ArrayList<>();
        orderDetails = orderService.listAll();

        model.addAttribute("orders",orderDetails);
        return "orders/orders";
    }

    @RequestMapping("orders/items")
    public String listItems(Model model){
        List<OrderDetail>  orderDetails = new ArrayList<>();
        orderDetails = orderService.listAll();

        model.addAttribute("orders",orderDetails);

        //model.addAttribute("items",orderService.listAll());
       // orderService.listAll().forEach(orderDetail -> System.out.println(orderDetail));
        return "orders/items";
    }

    @RequestMapping("orders/items/{id}")
    public String viewItems(@PathVariable("id") Long id, Model model){

        OrderDetail orderDetail = new OrderDetail();
        orderDetail = orderService.getById(id);

        model.addAttribute("order",orderDetail);

        return "orders/itemDetails";


    }

   /* @RequestMapping("orders/items")
    public List listItems(){
        List orderDetails = new ArrayList<>();
        orderDetails = orderService.listAll();

        return orderDetails;
    }*/
}
