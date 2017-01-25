package org.renci.binning.incidental.uncseq.commons;

import org.renci.binning.core.BinningException;
import org.renci.binning.core.incidental.AbstractAnnotateVariantsCallable;
import org.renci.binning.dao.BinningDAOBeanService;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.dao.jpa.BinningDAOManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnnotateVariantsCallable extends AbstractAnnotateVariantsCallable {

    private static final Logger logger = LoggerFactory.getLogger(AnnotateVariantsCallable.class);

    public AnnotateVariantsCallable(BinningDAOBeanService daoBean, IncidentalBinningJob binningJob) {
        super(daoBean, binningJob);
    }

    // exists for testing
    public static void main(String[] args) {
        try {
            BinningDAOManager daoMgr = BinningDAOManager.getInstance();
            IncidentalBinningJob binningJob = daoMgr.getDAOBean().getIncidentalBinningJobDAO().findById(4218);
            AnnotateVariantsCallable callable = new AnnotateVariantsCallable(daoMgr.getDAOBean(), binningJob);
            callable.call();
        } catch (BinningDAOException | BinningException e) {
            e.printStackTrace();
        }
    }

}
