package fefo.springframeworkftp.spring4ftpapp.command;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
@NoArgsConstructor
public class OrderCommand {
    private Long id;

    private String branchCode;
    private String ord;
    private String eiclientNumber;
    private String clientOrderNumber;
    private String consigneeP;
    private String paymentTerms;
    private String earliestShipDate;
    private String latestShipDate;
    private String consigneeCode;
    private String billToCode;
    private String thirdPartyBillToCustomerNo;
    private String preferredCarrier;
    private String usd1;
    private String usd2;
    private Set<ItemsCommand> itemDetails = new HashSet<>();
}
