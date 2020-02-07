package fefo.springframeworkftp.spring4ftpapp.controllers;


import fefo.springframeworkftp.spring4ftpapp.command.BranchForm;
import fefo.springframeworkftp.spring4ftpapp.configuration.FTPIntegration;
import fefo.springframeworkftp.spring4ftpapp.converters.BranchFormToBranch;
import fefo.springframeworkftp.spring4ftpapp.converters.BranchToBranchForm;
import fefo.springframeworkftp.spring4ftpapp.model.Branch;
import fefo.springframeworkftp.spring4ftpapp.services.BranchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.validation.Valid;
import java.io.IOException;

@Controller
public class BranchController {

    private BranchService branchService;
    private BranchToBranchForm branchToBranchForm;
    private BranchFormToBranch branchFormToBranch;

    private Branch branch;

    @Autowired
    private FTPIntegration ftpIntegration;

    @Autowired
    private IntegrationFlowContext flowContext;

    @Autowired
    public void setBranchService(BranchService branchService) {
        this.branchService = branchService;
    }

    @Autowired
    public void setBranchToBranchForm(BranchToBranchForm branchToBranchForm) {
        this.branchToBranchForm = branchToBranchForm;
    }

    @Autowired
    public void setBranchFormToBranch(BranchFormToBranch branchFormToBranch) {
        this.branchFormToBranch = branchFormToBranch;
    }

    @RequestMapping( "/")
    public String branch(){return "redirect:/branch/list";}

    @RequestMapping({"/branch/list","/branch"})
    public String listBranches(Model model){
        model.addAttribute("branches",branchService.listAll());
        return "branch/list";
    }

    @RequestMapping("/branch/showbranch/{id}")
    public String getBranch (@PathVariable String id, Model model) throws IOException {
       model.addAttribute("branch", branchService.getById(Long.valueOf(id)));
       addFlowFtp(id);
        addFlowftpOutbound(id);
       return "branch/showbranch";
    }

    @RequestMapping("/branch/edit/{id}")
    public String edit(@PathVariable String id, Model model){
        Branch branch = branchService.getById(Long.valueOf(id));
        BranchForm branchForm = branchToBranchForm.convert(branch);
        model.addAttribute("branchForm",branchForm);
        return "branch/editbranchform";
    }

    @RequestMapping("/branch/branchform")
    public String newBranch(Model model){
        model.addAttribute("branchForm", new BranchForm());
         return "branch/branchform";
    }

    //@PostMapping
    @RequestMapping(value = "/branch/branchform", method = RequestMethod.POST)
    public String saveOrUpdateBranch(@Valid BranchForm branchForm, BindingResult bindingResult) throws IOException {

        if(bindingResult.hasErrors()){
            return "branch/branchform";
        }
        Branch savedBranch = branchService.saveOrUpdateBranchForm(branchForm);
        return "redirect:/branch/showbranch/" + savedBranch.getId();
    }

    @RequestMapping("/branch/delete/{id}")
    private String delete(@PathVariable String id){
        branchService.delete(Long.valueOf(id));
        flowContext.remove(id);
        flowContext.remove(id+"o");
        return "redirect:/branch/list";
    }

    private void addFlowFtp(String name) throws IOException {
        branch = branchService.getById(Long.valueOf(name));
        System.out.println(branch.getBranchCode());
        IntegrationFlow flow = ftpIntegration.fileInboundFlowFromFTPServer(branch);
        this.flowContext.registration(flow).id(name).autoStartup(true).register();
    }

    private void addFlowftpOutbound(String name) {
        branch = branchService.getById(Long.valueOf(name));
        System.out.println(branch.getBranchCode());
        IntegrationFlow flow = ftpIntegration.localToFtpFlow(branch);
        this.flowContext.registration(flow).id(name +"o").register();
    }

}