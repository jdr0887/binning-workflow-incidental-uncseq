package org.renci.binning.incidental.uncseq.executor;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;
import org.apache.commons.collections4.CollectionUtils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.renci.binning.dao.BinningDAOBeanService;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.dao.refseq.model.Variants_61_2;
import org.renci.binning.incidental.uncseq.commons.AnnotateVariantsCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnnotateVariantsDelegate implements JavaDelegate {

    private static final Logger logger = LoggerFactory.getLogger(AnnotateVariantsDelegate.class);

    public AnnotateVariantsDelegate() {
        super();
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        logger.debug("ENTERING execute(DelegateExecution)");

        Map<String, Object> variables = execution.getVariables();

        BundleContext bundleContext = FrameworkUtil.getBundle(getClass()).getBundleContext();
        ServiceReference<BinningDAOBeanService> daoBeanServiceReference = bundleContext.getServiceReference(BinningDAOBeanService.class);
        BinningDAOBeanService daoBean = bundleContext.getService(daoBeanServiceReference);

        Integer binningJobId = null;
        Object o = variables.get("binningJobId");
        if (o != null && o instanceof Integer) {
            binningJobId = (Integer) o;
        }

        try {
            IncidentalBinningJob binningJob = daoBean.getIncidentalBinningJobDAO().findById(binningJobId);
            binningJob.setStatus(daoBean.getIncidentalStatusTypeDAO().findById("Annotating variants"));
            daoBean.getIncidentalBinningJobDAO().save(binningJob);
            logger.info(binningJob.toString());

            List<Variants_61_2> variants = Executors.newSingleThreadExecutor().submit(new AnnotateVariantsCallable(daoBean, binningJob))
                    .get();
            if (CollectionUtils.isNotEmpty(variants)) {
                logger.info(String.format("saving %d Variants_61_2 instances", variants.size()));
                for (Variants_61_2 variant : variants) {
                    logger.info(variant.toString());
                    daoBean.getVariants_61_2_DAO().save(variant);
                }
            }

            binningJob = daoBean.getIncidentalBinningJobDAO().findById(binningJobId);
            binningJob.setStatus(daoBean.getIncidentalStatusTypeDAO().findById("Annotated variants"));
            daoBean.getIncidentalBinningJobDAO().save(binningJob);
            logger.info(binningJob.toString());

        } catch (Exception e) {
            try {
                IncidentalBinningJob binningJob = daoBean.getIncidentalBinningJobDAO().findById(binningJobId);
                binningJob.setStop(new Date());
                binningJob.setFailureMessage(e.getMessage());
                binningJob.setStatus(daoBean.getIncidentalStatusTypeDAO().findById("Failed"));
                daoBean.getIncidentalBinningJobDAO().save(binningJob);
                logger.info(binningJob.toString());
            } catch (BinningDAOException e1) {
                e1.printStackTrace();
            }
        }

    }

}
