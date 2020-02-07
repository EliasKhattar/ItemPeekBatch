package fefo.springframeworkftp.spring4ftpapp.batchreaders;

import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;
import fefo.springframeworkftp.spring4ftpapp.model.Order;
import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;
import org.springframework.batch.item.support.builder.SingleItemPeekableItemReaderBuilder;
import org.springframework.lang.Nullable;

import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

import static org.apache.logging.log4j.LogManager.getLogger;

public class OrderItemProcessingItemReader implements ItemReader<Order>, ItemStream{

    private static final Logger LOG = getLogger(OrderItemProcessingItemReader.class);

    private OrderDetail orderDetail;
    private boolean recordFinished;


    private FlatFileItemReader fieldSetReader;
    SingleItemPeekableItemReader<Order> reader = new SingleItemPeekableItemReaderBuilder<>().delegate(getFieldSetReader()).build();

    private String filePath;

    public OrderItemProcessingItemReader(String filePath) {
        this.filePath = filePath;
    }


    @Nullable
    @Override
    public Order read() throws Exception {
        OrderDetail t = null;
        /*while (!recordFinished){
            if ((Order) fieldSetReader.read() instanceof OrderDetail){
                t = (OrderDetail) fieldSetReader.read();
            }else if((Order) fieldSetReader.read() instanceof ItemDetail){
                if (t.getItemDetails() == null) {
                    t.setItemDetails(new ArrayList<>());
                }

                ItemDetail itemDetail = (ItemDetail) fieldSetReader.read();
                itemDetail.setOrderDetails(t);
                t.getItemDetails().add(itemDetail);
            }else if (reader.peek() instanceof OrderDetail){
                return t;
            }*/

        while (!recordFinished){
            if ((Order) reader.read() instanceof OrderDetail){
                t = (OrderDetail) reader.read();
            }else if((Order) reader.read() instanceof ItemDetail){
                if (t.getItemDetails() == null) {
                    t.setItemDetails(new ArrayList<>());
                }

                ItemDetail itemDetail = (ItemDetail) reader.read();
                itemDetail.setOrderDetails(t);
                t.getItemDetails().add(itemDetail);
            }else if (reader.peek() instanceof OrderDetail){
                return t;
            }


        }
        return null;
        /*recordFinished = false;
        while (!recordFinished){
            process((Order) fieldSetReader.read());

        }
        OrderDetail result = orderDetail;
        orderDetail = null;

        return result;*/
    }

    /**
     * Processing the records coming from batch reading and mapping them correcting before
     * jpa writes them to the DB.
     *
     */

    /*private void process(Order object) throws Exception{

        // Finish processing if we hit the end of the file.
        if (object == null) {
            LOG.debug("FINISHED");
            recordFinished = true;
            return;
        }

        if (object instanceof OrderDetail){
            orderDetail = (OrderDetail) object;

        }else if (object instanceof ItemDetail) {

            if (orderDetail.getItemDetails() == null) {
                orderDetail.setItemDetails(new ArrayList<>());
            }

            ItemDetail itemDetail = (ItemDetail) object;
            itemDetail.setOrderDetails(orderDetail);
            orderDetail.getItemDetails().add(itemDetail);

        }

        }*/


    public void setFieldSetReader(FlatFileItemReader<FieldSet> fieldSetReader) {
        this.fieldSetReader = fieldSetReader;
    }

    public FlatFileItemReader getFieldSetReader() {
        return fieldSetReader;
    }

    @Override
    public void close() throws ItemStreamException {
        this.fieldSetReader.close();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.fieldSetReader.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        this.fieldSetReader.update(executionContext);
    }
}
