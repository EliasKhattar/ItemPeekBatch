package fefo.springframeworkftp.spring4ftpapp.converters;

import fefo.springframeworkftp.spring4ftpapp.command.OrderCommand;
import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;
import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import lombok.Synchronized;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

public class OrderToOrderCommand implements Converter<OrderDetail, OrderCommand> {

    private final ItemsToItemsCommand itemsConverter;

    public OrderToOrderCommand(ItemsToItemsCommand itemsConverter) {
        this.itemsConverter = itemsConverter;
    }

    @Synchronized
    @Nullable
    @Override
    public OrderCommand convert(OrderDetail orderDetail) {

        if(orderDetail == null)
            return null;

        final OrderCommand orderCommand = new OrderCommand();
        orderCommand.setId(orderDetail.getId());
        orderCommand.setBranchCode(orderDetail.getBranchCode());
        orderCommand.setClientOrderNumber(orderDetail.getClientOrderNumber());
        orderCommand.setBillToCode(orderDetail.getBillToCode());
        orderCommand.setConsigneeCode(orderDetail.getConsigneeCode());
        orderCommand.setConsigneeP(orderDetail.getConsigneeP());
        orderCommand.setEarliestShipDate(orderDetail.getEarliestShipDate());
        orderCommand.setLatestShipDate(orderDetail.getLatestShipDate());
        orderCommand.setOrd(orderDetail.getOrd());
        orderCommand.setPaymentTerms(orderDetail.getPaymentTerms());
        orderCommand.setPreferredCarrier(orderDetail.getPreferredCarrier());
        orderCommand.setThirdPartyBillToCustomerNo(orderDetail.getThirdPartyBillToCustomerNo());
        orderCommand.setUsd1(orderDetail.getUsd1());
        orderCommand.setUsd2(orderDetail.getUsd2());

        if(orderDetail.getItemDetails() != null){
            orderDetail.getItemDetails()
                    .forEach((ItemDetail itemDetail)  -> orderCommand.getItemDetails().add(itemsConverter.convert(itemDetail)));
        }

        return orderCommand;
    }
}
