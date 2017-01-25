package org.renci.binning.incidental.uncseq.commands;

import static org.renci.binning.core.Constants.BINNING_HOME;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.renci.binning.dao.BinningDAOBeanService;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.incidental.uncseq.commons.LoadVCFCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(scope = "diagnostic-uncseq", name = "load-vcf", description = "Load VCF")
@Service
public class LoadVCFAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(LoadVCFAction.class);

    @Reference
    private BinningDAOBeanService binningDAOBeanService;

    @Option(name = "--binningJobId", description = "IncidentalBinningJob Identifier", required = true, multiValued = false)
    private Integer binningJobId;

    public LoadVCFAction() {
        super();
    }

    @Override
    public Object execute() throws Exception {
        logger.debug("ENTERING execute()");

        IncidentalBinningJob binningJob = binningDAOBeanService.getIncidentalBinningJobDAO().findById(binningJobId);
        logger.info(binningJob.toString());

        try {
            binningJob.setStatus(binningDAOBeanService.getIncidentalStatusTypeDAO().findById("VCF loading"));
            binningDAOBeanService.getIncidentalBinningJobDAO().save(binningJob);

            String binningHome = System.getenv(BINNING_HOME);

            ExecutorService es = Executors.newSingleThreadExecutor();
            es.submit(new LoadVCFCallable(binningDAOBeanService, binningJob, binningHome));
            es.shutdown();
            es.awaitTermination(1L, TimeUnit.DAYS);

            binningJob.setStatus(binningDAOBeanService.getIncidentalStatusTypeDAO().findById("VCF loaded"));
            binningDAOBeanService.getIncidentalBinningJobDAO().save(binningJob);

        } catch (Exception e) {
            try {
                binningJob.setStop(new Date());
                binningJob.setFailureMessage(e.getMessage());
                binningJob.setStatus(binningDAOBeanService.getIncidentalStatusTypeDAO().findById("Failed"));
                binningDAOBeanService.getIncidentalBinningJobDAO().save(binningJob);
            } catch (BinningDAOException e1) {
                e1.printStackTrace();
            }
        }
        return null;
    }

}
