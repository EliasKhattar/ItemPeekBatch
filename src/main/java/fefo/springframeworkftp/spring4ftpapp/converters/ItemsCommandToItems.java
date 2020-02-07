package fefo.springframeworkftp.spring4ftpapp.converters;

import fefo.springframeworkftp.spring4ftpapp.command.ItemsCommand;
import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;
import lombok.Synchronized;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

public class ItemsCommandToItems implements Converter<ItemsCommand,ItemDetail> {



    @Synchronized
    @Nullable
    @Override
    public ItemDetail convert(ItemsCommand itemsCommand) {

        if(itemsCommand == null)
        return null;

        final ItemDetail itemDetail = new ItemDetail();

        itemDetail.setId(itemsCommand.getId());
        itemDetail.setBranchCode(itemsCommand.getBranchCode());
        itemDetail.setSkuNumber(itemsCommand.getSkuNumber());
        itemDetail.setReceiptPo(itemsCommand.getReceiptPo());
        itemDetail.setDetail(itemsCommand.getDetail());
        itemDetail.setLineNumber(itemsCommand.getLineNumber());
        itemDetail.setLotNo(itemsCommand.getLotNo());
        //itemDetail.setOrdNo(itemsCommand.getOrdNo());
        itemDetail.setQty(itemsCommand.getQty());
        itemDetail.setUsd1(itemsCommand.getUsd1());
        itemDetail.setUsd2(itemsCommand.getUsd2());
        itemDetail.setUsd3(itemsCommand.getUsd3());
        itemDetail.setUsd4(itemsCommand.getUsd4());
        itemDetail.setUsd5(itemsCommand.getUsd5());

        return itemDetail;
    }
}
