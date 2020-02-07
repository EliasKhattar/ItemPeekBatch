package fefo.springframeworkftp.spring4ftpapp.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.integration.dsl.IntegrationFlow;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.validation.constraints.Size;

@Entity
@AllArgsConstructor
@NoArgsConstructor
public class Branch {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long _id;
    private String branchCode;
    private String host;
    private Integer ftpPort;
    private String usern;
    private String folderPath;
    private String password;

/*    public Branch(String branchCode, String host, String user, String password) {
        this.branchCode = branchCode;
        this.host = host;
        this.user = user;
        this.password = password;
    }

    public Branch() {

    }*/


    public Long getId() {
        return _id;
    }

    public void setId(Long id) {
        this._id = id;
    }

    public String getBranchCode() {
        return branchCode;
    }

    public void setBranchCode(String branchCode) {
        this.branchCode = branchCode;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getFtpPort() {
        return ftpPort;
    }

    public void setFtpPort(Integer ftpPort) {
        this.ftpPort = ftpPort;
    }

    public String getUsern() {
        return usern;
    }

    public String getFolderPath() {
        return folderPath;
    }

    public void setFolderPath(String folderPath) {
        this.folderPath = folderPath;
    }

    public void setUser(String usern) {
        this.usern = usern;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
