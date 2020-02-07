package fefo.springframeworkftp.spring4ftpapp.converters;

import fefo.springframeworkftp.spring4ftpapp.command.BranchForm;
import fefo.springframeworkftp.spring4ftpapp.model.Branch;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Component
public class BranchToBranchForm implements Converter<Branch, BranchForm>{


    @Override
    public BranchForm convert(Branch branch) {
        BranchForm branchForm = new BranchForm();
        branchForm.setBranchCode(branch.getBranchCode());
        branchForm.setHost(branch.getHost());
        branchForm.setFtpPort(branch.getFtpPort());
        branchForm.setId(branch.getId());
        branchForm.setPassword(branch.getPassword());
        branchForm.setUsern(branch.getUsern());
        branchForm.setFolderPath(branch.getFolderPath());

        return branchForm;
    }
}
