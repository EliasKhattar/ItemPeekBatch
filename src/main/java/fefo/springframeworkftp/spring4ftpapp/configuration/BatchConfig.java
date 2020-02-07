package fefo.springframeworkftp.spring4ftpapp.configuration;

import fefo.springframeworkftp.spring4ftpapp.batchreaders.OrderItemProcessingItemReader;
import fefo.springframeworkftp.spring4ftpapp.components.JobCompletionNotificationListener;
import fefo.springframeworkftp.spring4ftpapp.model.ItemDetail;
import fefo.springframeworkftp.spring4ftpapp.model.Order;
import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private EntityManagerFactory emf;

    @Bean
    public Step orderStep() throws Exception {
        return stepBuilderFactory.get("orderStep")
                .<Order,Order>chunk(5)
                .reader(reader(null))
                //.writer(writer(null))
                .writer(jpaItemWriter())
                .build();

    }

    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step sampleStep) {
        return jobBuilderFactory.get("orderJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(sampleStep)
                .end()
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader readers(@Value("#{jobParameters[file_path]}") String filePath) throws Exception {

        final FileSystemResource fileResource = new FileSystemResource(filePath);
        return new FlatFileItemReaderBuilder()
                .name("ordersItemReader")
                .resource(fileResource)
                .lineMapper(orderLineMapper())
                .build();
    }

    @Bean
    @StepScope
    public OrderItemProcessingItemReader reader(@Value("#{jobParameters[file_path]}") String filePath) throws Exception {

        final FileSystemResource fileResource = new FileSystemResource(filePath);
        String decNo = fileResource.getFilename().substring(0,10);
        OrderItemProcessingItemReader orderItemProcessingItemReader = new OrderItemProcessingItemReader(decNo);
        orderItemProcessingItemReader.setFieldSetReader(readers(null));

        return orderItemProcessingItemReader;
    }

    @Bean
    @Primary
    public JpaItemWriter jpaItemWriter(){
        JpaItemWriter writer = new JpaItemWriter();
        writer.setEntityManagerFactory(emf);
        return writer;
    }


   /* @Bean
    @StepScope
    public ItemWriter<Order> writer(@Value("#{jobParameters[file_path]}") String filePath) throws Exception {
        final FileSystemResource fileResource = new FileSystemResource(filePath);
        String branchCode = fileResource.getFilename().substring(5,8);
        return new JpaOrderItemWriter(branchCode);
    }*/


    @Bean
    public PatternMatchingCompositeLineMapper orderLineMapper() throws Exception {

        PatternMatchingCompositeLineMapper mapper = new PatternMatchingCompositeLineMapper();

        Map<String, LineTokenizer> tokenizers = new HashMap<String, LineTokenizer>();
        tokenizers.put("ORD*",orderTokenizer() );
        tokenizers.put("DET*",orderItemTokenizer());

        mapper.setTokenizers(tokenizers);

        Map<String, FieldSetMapper> mappers = new HashMap<String, FieldSetMapper>();
        mappers.put("ORD*", orderFieldSetMapper());
        mappers.put("DET*", orderLineFieldSetMapper());

        mapper.setFieldSetMappers(mappers);

        return mapper;

    }


    @Bean
    public LineTokenizer orderTokenizer() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
        tokenizer.setNames(new String[] { "ord", "eiclientNumber", "clientOrderNumber", "consigneeP", "paymentTerms", "earliestShipDate", "latestShipDate", "consigneeCode", "billToCode", "thirdPartyBillToCustomerNo", "preferredCarrier", "usd1","usd2"});
        return tokenizer;
    }

    @Bean
    public FieldSetMapper<OrderDetail> orderFieldSetMapper() throws Exception {
        BeanWrapperFieldSetMapper<OrderDetail> mapper =
                new BeanWrapperFieldSetMapper<OrderDetail>();

        mapper.setPrototypeBeanName("orderDetail");
        mapper.afterPropertiesSet();
        return mapper;
    }


    @Bean
    @Scope("prototype")
    public OrderDetail orderDetail() {
        return new OrderDetail();
    }

    @Bean
    public LineTokenizer orderItemTokenizer() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
        tokenizer.setNames(new String[] { "detail", "lineNumber", "skuNumber", "qty", "lotNo", "receiptPo", "usd1", "usd2", "usd3", "usd4", "usd5"});
        return tokenizer;
    }

    @Bean
    public FieldSetMapper<ItemDetail> orderLineFieldSetMapper() throws Exception {
        BeanWrapperFieldSetMapper<ItemDetail> mapper =
                new BeanWrapperFieldSetMapper<ItemDetail>();

        mapper.setPrototypeBeanName("itemDetail");
        mapper.afterPropertiesSet();
        return mapper;
    }

    @Bean
    @Scope("prototype")
    public ItemDetail itemDetail() {
        return new ItemDetail();
    }

   /* @Bean
    public Step orderStep(JdbcBatchItemWriter<OrderDetail> writer){
        return stepBuilderFactory.get("orders")
                .<OrderDetail,OrderDetail>chunk(5)
                .reader(reader(null))
                .writer(writer)
                .build();
    }

    @Bean
    public Job importOrderJob(JobCompletionNotificationListener listener, Step sampleStep){

        return jobBuilderFactory.get("importOrderJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(sampleStep)
                .end()
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<OrderDetail> writer(DataSource dataSource,@Value("#{jobParameters[file_path]}") String filePath){

        final FileSystemResource fileResource = new FileSystemResource(filePath);
        String branchCode = fileResource.getFilename().substring(5,8); //getting the branch code from the path to insert it in table order details

        return new JdbcBatchItemWriterBuilder<OrderDetail>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO order_detail (ord, branch_code, eiclient_number, client_order_number, consigneep, payment_terms, earliest_ship_date, latest_ship_date, consignee_code, bill_to_code, third_party_bill_to_customer_no, preferred_carrier, usd1, usd2) " +
                        "VALUES (:ord, '"+branchCode+"', :eiclientNumber, :clientOrderNumber, :consigneeP, :paymentTerms, :earliestShipDate, :latestShipDate, :consigneeCode, :billToCode, :thirdPartyBillToCustomerNo, :preferredCarrier, :usd1, :usd2)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<OrderDetail> reader(@Value("#{jobParameters[file_path]}") String filePath) {

        final FileSystemResource fileResource = new FileSystemResource(filePath);

        return new FlatFileItemReaderBuilder<OrderDetail>()
                .name("ordersItemReader")
                .resource(fileResource)
                .delimited()
                .names(new String[]{"ord", "branchCode", "eiclientNumber", "clientOrderNumber", "consigneeP", "paymentTerms", "earliestShipDate", "latestShipDate", "consigneeCode", "billToCode", "thirdPartyBillToCustomerNo", "preferredCarrier", "usd1","usd2"})

                .fieldSetMapper(new BeanWrapperFieldSetMapper<OrderDetail>() {{
                    setTargetType(OrderDetail.class);
                }})

                .build();
    }


    @Bean
    public Step orderStepReading(){

        return stepBuilderFactory.get("orderStep")
                .<String,String>chunk(5)
                .reader(itemReader(null))
                .writer(i -> i.stream().forEach(j -> System.out.println(j)))
                .build();
    }*/


  /*  @Bean
    public Job orderJob(){
        return jobBuilderFactory.get("orderJob")
                .incrementer(new RunIdIncrementer())
                .start(orderStep())
                .build();
    }
    */


   /* @Bean
    @StepScope
    public FlatFileItemReader<String> itemReader(@Value("#{jobParameters[file_path]}") String filePath) {
        FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
        //final FileSystemResource fileResource = new FileSystemResource(filePath);
        reader.setResource(new FileSystemResource(filePath));
        reader.setLineMapper(new PassThroughLineMapper());
        return reader;

    }*/
}
