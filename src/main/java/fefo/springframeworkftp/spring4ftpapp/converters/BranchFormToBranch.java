package fefo.springframeworkftp.spring4ftpapp.converters;


import fefo.springframeworkftp.spring4ftpapp.command.BranchForm;
import fefo.springframeworkftp.spring4ftpapp.model.Branch;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;


@Component
public class BranchFormToBranch implements Converter<BranchForm, Branch> {

    @Override
    public Branch convert(BranchForm branchForm) {
        Branch branch = new Branch();
        if(branchForm.getId() != null && !StringUtils.isEmpty(branchForm.getId())){
            branch.setId(new Long(branchForm.getId()));
        }
        branch.setBranchCode(branchForm.getBranchCode());
        branch.setHost(branchForm.getHost());
        branch.setFtpPort(branchForm.getFtpPort());
        branch.setPassword(branchForm.getPassword());
        branch.setUser(branchForm.getUsern());
        branch.setFolderPath(branchForm.getFolderPath());

        return branch;
    }
}
