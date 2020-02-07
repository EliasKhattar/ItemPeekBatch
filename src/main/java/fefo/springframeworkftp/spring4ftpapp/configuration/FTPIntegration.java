package fefo.springframeworkftp.spring4ftpapp.configuration;

import fefo.springframeworkftp.spring4ftpapp.csvprocessing.CSVToCSVNoQ;
import fefo.springframeworkftp.spring4ftpapp.model.Branch;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import fefo.springframeworkftp.spring4ftpapp.repository.BranchRepository;
import org.aopalliance.aop.Advice;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.dsl.SourcePollingChannelAdapterSpec;
import org.springframework.integration.file.FileNameGenerator;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.*;
import org.springframework.integration.file.remote.session.*;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.ftp.dsl.Ftp;
import org.springframework.integration.ftp.dsl.FtpInboundChannelAdapterSpec;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.handler.advice.ExpressionEvaluatingRequestHandlerAdvice;
import org.springframework.integration.jdbc.metadata.JdbcMetadataStore;

import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.scheduling.PollerMetadata;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

@Configuration
@EnableIntegration
@ComponentScan
public class FTPIntegration {

    public static final String TEMPORARY_FILE_SUFFIX = ".part";
    public static final int POLLER_FIXED_PERIOD_DELAY = 5000;
    public static final int MAX_MESSAGES_PER_POLL = 100;

    private DataSource dataSource;

    private static final Logger LOG = Logger.getLogger(FTPIntegration.class);
    @Autowired
    private CSVToCSVNoQ csvToCSVNoQ;

    public FTPIntegration() {
    }

    @Autowired
    private Job orderJob;

    @Autowired
    private JobRepository jobRepository;

/*    @Autowired
    BranchRepository branchRepository;*/

    Map<Object, SessionFactory<FTPFile>> factories = new HashMap<>();
    DefaultSessionFactoryLocator<FTPFile> defaultSessionFactoryLocator = new DefaultSessionFactoryLocator<FTPFile>(factories);

    @Bean
    public Branch myBranch() {
        return new Branch();
    }

    /**
     * The default poller with 5s, 100 messages , will poll the FTP folder location
     *
     * @return default poller.
     */
    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata poller() {
        return Pollers
                .fixedDelay(POLLER_FIXED_PERIOD_DELAY)
                .maxMessagesPerPoll(MAX_MESSAGES_PER_POLL)
                .transactional()
                .get();
    }

    /**
     * The direct channel for the flow.
     *
     * @return MessageChannel
     */
    @Bean
    public MessageChannel stockIntermediateChannel() {
        return new DirectChannel();
    }

    /**
     * Method that creates a flow to read from FTP server the csv file
     * and transform it to a local folder with the name of the branch.
     *
     * @return IntegrationFlow
     */

    public IntegrationFlow fileInboundFlowFromFTPServer(Branch myBranch) throws IOException {

        final FtpInboundChannelAdapterSpec sourceSpecFtp = Ftp.inboundAdapter(createNewFtpSessionFactory(myBranch))
                .preserveTimestamp(true)
                //.patternFilter("*.csv")
                .maxFetchSize(MAX_MESSAGES_PER_POLL)
                .remoteDirectory(myBranch.getFolderPath())
                .regexFilter("FEFOexport" + myBranch.getBranchCode() + ".csv")
                .deleteRemoteFiles(true)
                .localDirectory(new File(myBranch.getBranchCode()))
                .temporaryFileSuffix(TEMPORARY_FILE_SUFFIX);

        // Poller definition
        final Consumer<SourcePollingChannelAdapterSpec> stockInboundPoller = endpointConfigurer -> endpointConfigurer
                //.id("stockInboundPoller")
                .autoStartup(true)
                .poller(poller());

        IntegrationFlow flow = IntegrationFlows
                .from(sourceSpecFtp, stockInboundPoller)

                .transform(File.class, p -> {
                    // log step
                    LOG.info("flow=stockInboundFlowFromAFT, message=incoming file: " + p);
                    return p;
                })
                .handle(m -> {
                    try {/* Invoking a method through the integration flow that reads a csv file and transform it to a new format that should be sent to FTP */
                        this.csvToCSVNoQ.writeCSVfinal("test", myBranch.getBranchCode() + "/final" + myBranch.getBranchCode() + ".csv", myBranch.getBranchCode() + "/FEFOexport" + myBranch.getBranchCode() + ".csv");
                        LOG.info("Writing final file .csv " + m);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                })
                .get();

        return flow;
    }

    /**
     * Creating the outbound adaptor to send files from local to FTP server
     *@return integration flow from local to FTP server
     */

    public IntegrationFlow localToFtpFlow(Branch myBranch) {

        return IntegrationFlows.from(Files.inboundAdapter(new File(myBranch.getBranchCode()))
                        .filter(new ChainFileListFilter<File>()
                                .addFilter(new RegexPatternFileListFilter("final" + myBranch.getBranchCode() + ".csv"))
                                .addFilter(new FileSystemPersistentAcceptOnceFileListFilter(metadataStore(dataSource), "foo"))),//FileSystemPersistentAcceptOnceFileListFilter
                e -> e.poller(Pollers.fixedDelay(10_000)))
                .enrichHeaders(h ->h.headerExpression("file_originalFile", "new java.io.File('"+ myBranch.getBranchCode() +"/FEFOexport" + myBranch.getBranchCode() + ".csv')",true))
                .transform(p -> {
                    LOG.info("Sending file " + p + " to FTP branch " + myBranch.getBranchCode());
                    return p;
                })

                //.log()
                .transform(m -> {
                            this.defaultSessionFactoryLocator.addSessionFactory(myBranch.getBranchCode(),createNewFtpSessionFactory(myBranch));
                            LOG.info("Adding factory to delegation");
                            return m;
                })
                .publishSubscribeChannel(s ->
                        s.subscribe(h -> h.handle(Ftp.outboundAdapter(createNewFtpSessionFactory(myBranch), FileExistsMode.REPLACE)
                                         .useTemporaryFileName(true)
                                         .autoCreateDirectory(false)
                                         .remoteDirectory(myBranch.getFolderPath()), e -> e.advice(expressionAdvice())))
                         .subscribe(f ->f.transform(fileMessageToJobRequest())
                                        .handle(jobLaunchingGateway()).channel("nullChannel")))
                .handle(jobExecution -> {
                    System.out.println("Payload is: " + jobExecution.getPayload());
                })
                /*.handle(Ftp.outboundAdapter(createNewFtpSessionFactory(myBranch), FileExistsMode.REPLACE)
                        .useTemporaryFileName(true)
                        .autoCreateDirectory(false)
                        .remoteDirectory(myBranch.getFolderPath()), e -> e.advice(expressionAdvice()))*/
                .get();
    }

    @Bean
    public FileMessageToJobRequest fileMessageToJobRequest(){
        FileMessageToJobRequest fileMessageToJobRequest = new FileMessageToJobRequest();
        fileMessageToJobRequest.setFileParameterName("file_path");
        fileMessageToJobRequest.setJob(orderJob);
        return fileMessageToJobRequest;
    }

    //@Bean
    public JobLaunchingGateway jobLaunchingGateway() {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        simpleJobLauncher.setTaskExecutor(new SyncTaskExecutor());
        JobLaunchingGateway jobLaunchingGateway = new JobLaunchingGateway(simpleJobLauncher);

        return jobLaunchingGateway;
    }

    /**
    * Creating the advice for routing the payload of the outbound message on different expressions (success, failure)
    * @return Advice
    */

    @Bean
    public Advice expressionAdvice() {
        ExpressionEvaluatingRequestHandlerAdvice advice = new ExpressionEvaluatingRequestHandlerAdvice();
        advice.setSuccessChannelName("success.input");
        advice.setOnSuccessExpressionString("payload.delete() + ' was successful'");
        //advice.setOnSuccessExpressionString("inputMessage.headers['file_originalFile'].renameTo(new java.io.File(payload.absolutePath + '.success.to.send'))");
        //advice.setFailureChannelName("failure.input");
        advice.setOnFailureExpressionString("payload + ' was bad, with reason: ' + #exception.cause.message");
        advice.setTrapException(true);
        return advice;
    }

    /**
     * Creating FTP connection based on the branch ftp data entered.
     * @return ftpSessionFactory
     */

    public DefaultFtpSessionFactory createNewFtpSessionFactory(Branch branch) {
        final DefaultFtpSessionFactory factory = new DefaultFtpSessionFactory();
        factory.setHost(branch.getHost());
        factory.setUsername(branch.getUsern());
        factory.setPort(branch.getFtpPort());
        factory.setPassword(branch.getPassword());
        return factory;
    }

    /**
     * Creating a default FTP connection.
     * @return ftpSessionFactory
     */
    @Bean
    public SessionFactory<FTPFile> createNewFtpSessionFactory() {
        final DefaultFtpSessionFactory factory = new DefaultFtpSessionFactory();
        factory.setHost("bey-notes-fs.bey.ei");
        factory.setUsername("bey-ftp");
        factory.setPort(21);
        factory.setPassword("spicysilk");
        return factory;
    }

    /**
     * Creating a metadata store to be used across the application flows to prevent reprocessing the file if it is already processed.
     * This will save the new file in a metadata table in the DB with the state of the report, so when a new copy comes with different date it will be processed only.
     * @return metadataStore
     */
    @Bean
    public ConcurrentMetadataStore metadataStore(final DataSource dataSource) {
        return new JdbcMetadataStore(dataSource);
    }

    /**
     * Success channel that will handle the AdviceMessage from the outbound adapter in method localToFtpFlow
     * and sends the inputMessage file_originalFile to FTP destination folder specified.
     * @return integration flow
     */

    @Bean
    public IntegrationFlow success(){
        return f -> f.transform("inputMessage.headers['file_originalFile']")
                .transform(e -> {
                    //getting the Branch code from the Input message and calling the correct factory based on it
                    delegatingSessionFactoryAuto().setThreadKey(e.toString().substring(0,3));
                    return e;
                })
                .handle(Ftp.outboundAdapter(delegatingSessionFactoryAuto(), FileExistsMode.REPLACE)
                        .useTemporaryFileName(true)
                        .autoCreateDirectory(true)
                        .fileNameGenerator(new FileNameGenerator() {
                            @Override
                            public String generateFileName(Message<?> message) {
                                return new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss").format(new Date()) + ".csv";
                            }
                        })
                        .remoteDirectory("/ftp/erbranch/EDMS/FEFO/History/" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())) // + dateFormat.format(date)
                        .get(),e -> e.advice(expressionAdviceForSuccess()));

    }

    /**
     * Second advice that will delete the original file from local after sending it to History folder at FTP branch.
     * Used in the second outboundAdapter in the success channel.
     * @return Advice
     * */

    @Bean
    public Advice expressionAdviceForSuccess() {
        ExpressionEvaluatingRequestHandlerAdvice advice = new ExpressionEvaluatingRequestHandlerAdvice();
        advice.setOnSuccessExpressionString("payload.delete() + ' was successful'");
        advice.setOnFailureExpressionString("payload + ' was bad, with reason: ' + #exception.cause.message");
        advice.setTrapException(true);
        return advice;
    }

    @Bean
    public DelegatingSessionFactory<FTPFile> delegatingSessionFactoryAuto(){

        SessionFactoryLocator<FTPFile> sff = createNewFtpSessionFactoryAndAddItToTheLocator();
        return new DelegatingSessionFactory<FTPFile>(sff);
    }

    @Bean
    public SessionFactoryLocator<FTPFile> createNewFtpSessionFactoryAndAddItToTheLocator(){

        this.defaultSessionFactoryLocator.addSessionFactory("BEY",createNewFtpSessionFactory());
        return this.defaultSessionFactoryLocator;
    }
}