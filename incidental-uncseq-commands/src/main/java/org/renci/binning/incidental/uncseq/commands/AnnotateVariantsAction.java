package org.renci.binning.incidental.uncseq.commands;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.renci.binning.dao.BinningDAOBeanService;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.dao.refseq.model.Variants_61_2;
import org.renci.binning.incidental.uncseq.commons.AnnotateVariantsCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(scope = "incidental-ncgenes", name = "annotate-variants", description = "Annotate Variants")
@Service
public class AnnotateVariantsAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(AnnotateVariantsAction.class);

    @Reference
    private BinningDAOBeanService binningDAOBeanService;

    @Option(name = "--binningJobId", description = "IncidentalBinningJob Identifier", required = true, multiValued = false)
    private Integer binningJobId;

    public AnnotateVariantsAction() {
        super();
    }

    @Override
    public Object execute() throws Exception {
        logger.debug("ENTERING execute()");

        IncidentalBinningJob binningJob = binningDAOBeanService.getIncidentalBinningJobDAO().findById(binningJobId);
        logger.info(binningJob.toString());

        try {
            binningJob.setStatus(binningDAOBeanService.getIncidentalStatusTypeDAO().findById("Annotating variants"));
            binningDAOBeanService.getIncidentalBinningJobDAO().save(binningJob);

            List<Variants_61_2> variants = Executors.newSingleThreadExecutor()
                    .submit(new AnnotateVariantsCallable(binningDAOBeanService, binningJob)).get();
            if (CollectionUtils.isNotEmpty(variants)) {
                logger.info(String.format("saving %d Variants_61_2 instances", variants.size()));
                for (Variants_61_2 variant : variants) {
                    logger.info(variant.toString());
                    binningDAOBeanService.getVariants_61_2_DAO().save(variant);
                }
            }

            binningJob.setStatus(binningDAOBeanService.getIncidentalStatusTypeDAO().findById("Annotated variants"));
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

    public Integer getBinningJobId() {
        return binningJobId;
    }

    public void setBinningJobId(Integer binningJobId) {
        this.binningJobId = binningJobId;
    }

}
