package fefo.springframeworkftp.spring4ftpapp.components;

import fefo.springframeworkftp.spring4ftpapp.model.OrderDetail;
import lombok.AllArgsConstructor;
import org.apache.log4j.Logger;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private static final Logger LOG = Logger.getLogger(JobCompletionNotificationListener.class);

    private final JdbcTemplate jdbcTemplate;

    public void afterJob(JobExecution jobExecution){
        if(jobExecution.getStatus() == BatchStatus.COMPLETED){
            LOG.info("JOB Finished, verifying the results");

            jdbcTemplate.query("SELECT bill_to_code, branch_code, client_order_number, consignee_code, consigneep, earliest_ship_date, eiclient_number, latest_ship_date, ord, payment_terms, third_party_bill_to_customer_no, preferred_carrier, usd1, usd2 FROM order_detail",
                    (rs, row) -> new OrderDetail(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4),
                            rs.getString(5),
                            rs.getString(6),
                            rs.getString(7),
                            rs.getString(8),
                            rs.getString(9),
                            rs.getString(10),
                            rs.getString(11),
                            rs.getString(12),
                            rs.getString(13),
                            rs.getString(14)

                    )
            ).forEach(order -> System.out.println("Found <" + order + "> in the database."));
        }
        else {
            LOG.info("!!! JOB Has not FINISHED! Time to verify the results" + jobExecution.getStatus());
        }

        }
    }

