package org.renci.binning.incidental.uncseq.commands;

import java.io.File;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.renci.binning.dao.BinningDAOBeanService;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.DiagnosticBinningJob;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(scope = "incidental-uncseq", name = "create-binning-job", description = "Create Incidental UNCSeq Binning Job")
@Service
public class CreateBinningJobAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(CreateBinningJobAction.class);

    @Reference
    private BinningDAOBeanService binningDAOBeanService;

    @Option(name = "--gender", description = "Gender (M|F)", required = true, multiValued = false)
    private String gender;

    @Option(name = "--participant", description = "Participant", required = true, multiValued = false)
    private String participant;

    @Option(name = "--listVersion", description = "List Version", required = true, multiValued = false)
    private Integer listVersion;

    @Option(name = "--incidentalBinId", description = "IncidentalBin identifier", required = true, multiValued = false)
    private Integer incidentalBinId;

    @Option(name = "--vcf", description = "VCF", required = false, multiValued = false)
    private String vcf;

    public CreateBinningJobAction() {
        super();
    }

    @Override
    public Object execute() throws Exception {
        logger.debug("ENTERING execute()");

        IncidentalBinningJob binningJob = new IncidentalBinningJob();

        binningJob.setStudy("UNCSeq Cancer Study");
        binningJob.setStatus(binningDAOBeanService.getIncidentalStatusTypeDAO().findById("Requested"));

        binningJob.setGender(gender);
        binningJob.setParticipant(participant);
        binningJob.setListVersion(listVersion);

        if (StringUtils.isNotEmpty(vcf)) {
            File vcfFile = new File(vcf);
            if (!vcfFile.exists()) {
                throw new BinningDAOException("VCF not found: " + vcf);
            }
            binningJob.setVcfFile(vcf);
        }

        binningJob.setIncidentalBinX(binningDAOBeanService.getIncidentalBinXDAO().findById(incidentalBinId));
        logger.info(binningJob.getIncidentalBinX().toString());

        List<IncidentalBinningJob> foundBinningJobs = binningDAOBeanService.getIncidentalBinningJobDAO().findByExample(binningJob);
        if (CollectionUtils.isNotEmpty(foundBinningJobs)) {
            binningJob = foundBinningJobs.get(0);
        } else {
            binningJob.setId(binningDAOBeanService.getIncidentalBinningJobDAO().save(binningJob));
        }
        logger.info(binningJob.toString());

        return binningJob.toString();
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getParticipant() {
        return participant;
    }

    public void setParticipant(String participant) {
        this.participant = participant;
    }

    public Integer getListVersion() {
        return listVersion;
    }

    public void setListVersion(Integer listVersion) {
        this.listVersion = listVersion;
    }

    public Integer getIncidentalBinId() {
        return incidentalBinId;
    }

    public void setIncidentalBinId(Integer incidentalBinId) {
        this.incidentalBinId = incidentalBinId;
    }

    public String getVcf() {
        return vcf;
    }

    public void setVcf(String vcf) {
        this.vcf = vcf;
    }

}
