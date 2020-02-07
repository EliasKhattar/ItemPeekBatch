package fefo.springframeworkftp.spring4ftpapp.command;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
@NoArgsConstructor
public class ItemsCommand {

    private Long id;
    private String branchCode;
    private String ordNo;
    private String detail;
    private String lineNumber;
    private String skuNumber;
    private String qty;
    private String lotNo;
    private String receiptPo;
    private String usd1;
    private String usd2;
    private String usd3;
    private String usd4;
    private String usd5;
    private Set<OrderCommand> orderDetails = new HashSet<>();
}
