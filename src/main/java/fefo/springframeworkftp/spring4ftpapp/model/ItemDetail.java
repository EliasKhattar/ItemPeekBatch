package fefo.springframeworkftp.spring4ftpapp.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
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
public class ItemDetail extends Order{

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String branchCode;
    //private String ordNo;
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


    @ManyToOne
    @JoinColumn(name = "itemDetail_id")
    private OrderDetail orderDetails;

}
