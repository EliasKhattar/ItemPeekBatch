package fefo.springframeworkftp.spring4ftpapp.converters;

import fefo.springframeworkftp.spring4ftpapp.command.OrderCommand;
import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import lombok.Synchronized;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

public class OrderCommandToOrder implements Converter<OrderCommand,OrderDetail> {

    private final ItemsCommandToItems itemsConverter;

    public OrderCommandToOrder(ItemsCommandToItems itemsConverter) {
        this.itemsConverter = itemsConverter;
    }

    @Synchronized
    @Nullable
    @Override
    public OrderDetail convert(OrderCommand orderCommand) {
        if(orderCommand == null)
        return null;

        final OrderDetail orderDetail = new OrderDetail();

        orderDetail.setId(orderCommand.getId());
        orderDetail.setBranchCode(orderCommand.getBranchCode());
        orderDetail.setClientOrderNumber(orderCommand.getClientOrderNumber());
        orderDetail.setBillToCode(orderCommand.getBillToCode());
        orderDetail.setConsigneeCode(orderCommand.getConsigneeCode());
        orderDetail.setConsigneeP(orderCommand.getConsigneeP());
        orderDetail.setEarliestShipDate(orderCommand.getEarliestShipDate());
        orderDetail.setLatestShipDate(orderCommand.getLatestShipDate());
        orderDetail.setOrd(orderCommand.getOrd());
        orderDetail.setPaymentTerms(orderCommand.getPaymentTerms());
        orderDetail.setPreferredCarrier(orderCommand.getPreferredCarrier());
        orderDetail.setThirdPartyBillToCustomerNo(orderCommand.getThirdPartyBillToCustomerNo());
        orderDetail.setUsd1(orderCommand.getUsd1());
        orderDetail.setUsd2(orderCommand.getUsd2());

        if(orderCommand.getItemDetails() != null){
            orderCommand.getItemDetails()
                    .forEach(itemDetail -> orderDetail.getItemDetails().add(itemsConverter.convert(itemDetail)));
        }

        return orderDetail;
    }
}
