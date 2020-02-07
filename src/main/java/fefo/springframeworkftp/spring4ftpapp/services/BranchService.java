package fefo.springframeworkftp.spring4ftpapp.services;

import fefo.springframeworkftp.spring4ftpapp.command.BranchForm;
import fefo.springframeworkftp.spring4ftpapp.model.Branch;

import java.util.List;
import java.util.Optional;

public interface BranchService {

    List<Branch> listAll();

    Branch getById(Long id);

    Branch saveOrUpdate(Branch branch);

    void delete(Long id);

    Branch saveOrUpdateBranchForm(BranchForm branchForm);

    boolean isAvailable(Long id);


}
