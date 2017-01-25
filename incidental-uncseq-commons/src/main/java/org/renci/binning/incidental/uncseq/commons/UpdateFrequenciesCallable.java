package org.renci.binning.incidental.uncseq.commons;

import org.renci.binning.core.BinningException;
import org.renci.binning.core.incidental.AbstractUpdateFrequenciesCallable;
import org.renci.binning.dao.BinningDAOBeanService;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.dao.jpa.BinningDAOManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateFrequenciesCallable extends AbstractUpdateFrequenciesCallable {

    private static final Logger logger = LoggerFactory.getLogger(UpdateFrequenciesCallable.class);

    public UpdateFrequenciesCallable(BinningDAOBeanService daoBean, IncidentalBinningJob binningJob) {
        super(daoBean, binningJob);
    }

    public static void main(String[] args) {
        try {
            BinningDAOManager daoMgr = BinningDAOManager.getInstance();
            IncidentalBinningJob binningJob = daoMgr.getDAOBean().getIncidentalBinningJobDAO().findById(4218);
            UpdateFrequenciesCallable callable = new UpdateFrequenciesCallable(daoMgr.getDAOBean(), binningJob);
            callable.call();
        } catch (BinningDAOException | BinningException e) {
            e.printStackTrace();
        }
    }

}
