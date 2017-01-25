package org.renci.binning.incidental.uncseq.executor;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.renci.binning.dao.BinningDAOBeanService;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.incidental.uncseq.commons.UpdateIncidentalBinsCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateIncidentalBinsDelegate implements JavaDelegate {

    private static final Logger logger = LoggerFactory.getLogger(UpdateIncidentalBinsDelegate.class);

    public UpdateIncidentalBinsDelegate() {
        super();
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        logger.info("ENTERING execute(DelegateExecution)");
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
            binningJob.setStatus(daoBean.getIncidentalStatusTypeDAO().findById("Calculated incidental bin results"));
            daoBean.getIncidentalBinningJobDAO().save(binningJob);
            logger.info(binningJob.toString());

            ExecutorService es = Executors.newSingleThreadExecutor();
            Future<Void> future = es.submit(new UpdateIncidentalBinsCallable(execution.getVariables()));
            es.shutdown();
            es.awaitTermination(1L, TimeUnit.HOURS);

            binningJob = daoBean.getIncidentalBinningJobDAO().findById(binningJobId);
            binningJob.setStatus(daoBean.getIncidentalStatusTypeDAO().findById("Calculating incidental bin results"));
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
