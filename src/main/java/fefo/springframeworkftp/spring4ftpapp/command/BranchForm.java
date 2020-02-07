package fefo.springframeworkftp.spring4ftpapp.command;

import javax.validation.constraints.*;

public class BranchForm {

    private Long id;

    @Size(min = 3, max = 3, message = "Branch code must be 3 characters")
    private String branchCode;

    @Size(min = 7, max = 7, message = "Username should be 7 characters (ftp-bbb)")
    private String usern;

    @NotNull(message = "Password Should not be null, Please enter password")
    @Size(min = 2, message = "Password should be at least 2 characters")
    private String password;

    @NotBlank(message = "Host should not be null, please enter correct host")
    @Size(min = 2, message = "Host should be at least 2 characters")
    private String host;

    @NotNull(message = "Port should not be null")
    @Min(value = 2, message = "Port should be at least 2 characters")
    private Integer ftpPort;

    @NotBlank(message = "Folder Path should not be null")
    private String folderPath;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public void setUsern(String usern) {
        this.usern = usern;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
