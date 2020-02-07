package fefo.springframeworkftp.spring4ftpapp.model;


import lombok.*;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Entity
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Data
public class OrderDetail extends Order{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
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

    @OneToMany(mappedBy = "orderDetails", cascade = CascadeType.ALL,orphanRemoval = true)
    private List<ItemDetail> itemDetails = new ArrayList<>();

    public OrderDetail(String branchCode, String ord, String eiclientNumber, String clientOrderNumber, String consigneeP, String paymentTerms, String earliestShipDate, String latestShipDate, String consigneeCode, String billToCode, String thirdPartyBillToCustomerNo, String preferredCarrier, String usd1, String usd2) {
        this.branchCode = branchCode;
        this.ord = ord;
        this.eiclientNumber = eiclientNumber;
        this.clientOrderNumber = clientOrderNumber;
        this.consigneeP = consigneeP;
        this.paymentTerms = paymentTerms;
        this.earliestShipDate = earliestShipDate;
        this.latestShipDate = latestShipDate;
        this.consigneeCode = consigneeCode;
        this.billToCode = billToCode;
        this.thirdPartyBillToCustomerNo = thirdPartyBillToCustomerNo;
        this.preferredCarrier = preferredCarrier;
        this.usd1 = usd1;
        this.usd2 = usd2;
    }


    public OrderDetail(String branchCode, String ord, String eiclientNumber, String clientOrderNumber, String consigneeP, String paymentTerms, String earliestShipDate, String latestShipDate, String consigneeCode, String billToCode, String thirdPartyBillToCustomerNo,List<ItemDetail> itemDetails) {
        this.branchCode = branchCode;
        this.ord = ord;
        this.eiclientNumber = eiclientNumber;
        this.clientOrderNumber = clientOrderNumber;
        this.consigneeP = consigneeP;
        this.paymentTerms = paymentTerms;
        this.earliestShipDate = earliestShipDate;
        this.latestShipDate = latestShipDate;
        this.consigneeCode = consigneeCode;
        this.billToCode = billToCode;
        this.thirdPartyBillToCustomerNo = thirdPartyBillToCustomerNo;
        this.itemDetails = itemDetails;

    }

}
