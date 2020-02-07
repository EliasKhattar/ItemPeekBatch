package fefo.springframeworkftp.spring4ftpapp.converters;

import fefo.springframeworkftp.spring4ftpapp.command.ItemsCommand;
import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;
import lombok.Synchronized;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

public class ItemsToItemsCommand implements Converter<ItemDetail, ItemsCommand> {

    @Synchronized
    @Nullable
    @Override
    public ItemsCommand convert(ItemDetail itemDetail) {

        ItemsCommand itemsCommand = new ItemsCommand();
        itemsCommand.setBranchCode(itemDetail.getBranchCode());
        itemsCommand.setId(itemDetail.getId());
        itemsCommand.setDetail(itemDetail.getDetail());
        itemsCommand.setLineNumber(itemDetail.getLineNumber());
        itemsCommand.setLotNo(itemDetail.getLotNo());
        //itemsCommand.setOrdNo(itemDetail.getOrdNo());
        itemsCommand.setQty(itemDetail.getQty());
        itemsCommand.setReceiptPo(itemDetail.getDetail());
        itemsCommand.setSkuNumber(itemDetail.getLineNumber());
        itemsCommand.setUsd1(itemDetail.getUsd1());
        itemsCommand.setUsd2(itemDetail.getUsd2());
        itemsCommand.setUsd3(itemDetail.getUsd3());
        itemsCommand.setUsd4(itemDetail.getUsd4());

        return itemsCommand;
    }
}
