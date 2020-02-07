/*
package fefo.springframeworkftp.spring4ftpapp.configuration;

import fefo.springframeworkftp.spring4ftpapp.model.Branch;
import fefo.springframeworkftp.spring4ftpapp.repository.BranchRepository;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.dsl.SourcePollingChannelAdapterSpec;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.dsl.context.IntegrationFlowRegistration;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.file.remote.aop.RotatingServerAdvice;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.DelegatingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.ftp.dsl.Ftp;
import org.springframework.integration.ftp.dsl.FtpInboundChannelAdapterSpec;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.scheduling.PollerMetadata;

import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.lang.String.valueOf;

@Configuration
@Component
@EnableIntegration
public class SFTIntegration {

    public static final String TIMEZONE_UTC = "UTC";
    public static final String TIMESTAMP_FORMAT_OF_FILES = "yyyyMMddHHmmssSSS";
    public static final String TEMPORARY_FILE_SUFFIX = ".part";
    public static final int POLLER_FIXED_PERIOD_DELAY = 5000;
    public static final int MAX_MESSAGES_PER_POLL = 100;


    private static final Logger LOG = LoggerFactory.getLogger(SFTIntegration.class);
    private static final String CHANNEL_INTERMEDIATE_STAGE = "intermediateChannel";

    @Autowired
    private IntegrationFlowContext flowContext;

    */
/* pulling the server config from postgres DB*//*


    private final BranchRepository branchRepository;

    @Value("${app.temp-dir}")
    private String localTempPath;

    public SFTIntegration(BranchRepository branchRepository) {
        this.branchRepository = branchRepository;
    }

    */
/**
     * The default poller with 5s, 100 messages, RotatingServerAdvice and transaction.
     *
     * @return default poller.
     *//*

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata poller(){
        return Pollers
                .fixedDelay(POLLER_FIXED_PERIOD_DELAY)
                .advice(advice())
                .maxMessagesPerPoll(MAX_MESSAGES_PER_POLL)
                .transactional()
                .get();
    }

    */
/**
     * The direct channel for the flow.
     *
     * @return MessageChannel
     *//*

    @Bean
    public MessageChannel stockIntermediateChannel() {
        return new DirectChannel();
    }

    */
/**
     * Get the files from a remote directory. Add a timestamp to the filename
     * and write them to a local temporary folder.
     *
     * @return IntegrationFlow
     *//*

    @Bean
    public IntegrationFlow fileInboundFlowFromFTPServer(){

        final FtpInboundChannelAdapterSpec sourceSpecFtp = Ftp.inboundAdapter(delegatingFtpSessionFactory())
                .preserveTimestamp(true)
                .patternFilter("*.csv")
                .deleteRemoteFiles(true)
                .maxFetchSize(MAX_MESSAGES_PER_POLL)
                .remoteDirectory("/")
                .localDirectory(new File(localTempPath))
                .temporaryFileSuffix(TEMPORARY_FILE_SUFFIX)
                .localFilenameExpression(new FunctionExpression<String>(s -> {
                    final int fileTypeSepPos = s.lastIndexOf('.');
                    return DateTimeFormatter
                            .ofPattern(TIMESTAMP_FORMAT_OF_FILES)
                            .withZone(ZoneId.of(TIMEZONE_UTC))
                            .format(Instant.now())
                            + "_"
                            + s.substring(0,fileTypeSepPos)
                            + s.substring(fileTypeSepPos);
                }));

        // Poller definition
        final Consumer<SourcePollingChannelAdapterSpec> stockInboundPoller = endpointConfigurer -> endpointConfigurer
                .id("stockInboundPoller")
                .autoStartup(true)
                .poller(poller());

            IntegrationFlow flow = IntegrationFlows
                    .from(sourceSpecFtp, stockInboundPoller)
                    .transform(File.class, p ->{
                        // log step
                        LOG.info("flow=stockInboundFlowFromAFT, message=incoming file: " + p);
                        return p;
                    })
                    .channel(CHANNEL_INTERMEDIATE_STAGE)
                    .get();


        //this.flowContext.registration(flow).id(name).register().toString();
            this.flowContext.registration(flow).id("fileInb").register();
        return flow;
    }

    @Bean
    public IntegrationFlow stockIntermediateStageChannel() {
        IntegrationFlow flow = IntegrationFlows
                .from(CHANNEL_INTERMEDIATE_STAGE)
                .transform(p -> {
                    //log step
                    LOG.info("flow=stockIntermediateStageChannel, message=rename file: " + p);
                    return p;
                })
                //TODO
                .channel(new NullChannel())
                .get();
        return flow;

    }

    public DefaultFtpSessionFactory createNewFtpSessionFactory(final Branch branch){
        final DefaultFtpSessionFactory factory = new DefaultFtpSessionFactory();
        factory.setHost(branch.getHost());
        factory.setUsername(branch.getUsern());
        factory.setPort(branch.getFtpPort());
        factory.setPassword(branch.getPassword());
        return factory;
    }

    @Bean
    public DelegatingSessionFactory<FTPFile> delegatingFtpSessionFactory(){
        final List<Branch> branchConnections = new ArrayList<>();
        branchRepository.findAll().forEach(branchConnections::add);

        if(branchConnections.isEmpty()){
            return null;
        }

        final Map<Object, SessionFactory<FTPFile>> factories = new LinkedHashMap<>(10);

        for (Branch br : branchConnections) {
            // create a factory for every key containing server type, url and port
            if (factories.get(br.getId()) == null) {
                factories.put(br.getId().toString(), createNewFtpSessionFactory(br));
            }
        }

        LOG.info("Found " + factories.size() + " factories");
        // use the first SF as the default
        return new DelegatingSessionFactory<>(factories, factories.values().iterator().next());

    }

    @Bean
    public RotatingServerAdvice advice(){
        final List<Branch> branchConnections = new ArrayList<>();
        branchRepository.findAll().forEach(branchConnections::add);

        LOG.info("Found " + branchConnections.size() + " server entries for FEFO Stock.");

        if(branchConnections.isEmpty()){
            return null;
        }

        final List<RotatingServerAdvice.KeyDirectory> keyDirectories = new ArrayList<>();
        for (final Branch br : branchConnections) {
            keyDirectories
                    .add(new RotatingServerAdvice.KeyDirectory(br.getId().toString(), br.getFolderPath()));
        }

        LOG.info("Found key Directories " + keyDirectories.size() + " of them");
        */
/*final RotatingServerAdvice rot = new RotatingServerAdvice(
                new MyStandardRotationPolicy(delegatingSFtpSessionFactory(), keyDirectories, true,
                        getFilter(), partnerConfigRepo));
        return rot;*//*

        return new RotatingServerAdvice(delegatingFtpSessionFactory(), keyDirectories, true);

    }
}
*/
