package org.renci.binning.incidental.uncseq.ws;

import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.commons.collections4.CollectionUtils;
import org.renci.binning.core.BinningExecutorService;
import org.renci.binning.core.incidental.IncidentalBinningJobInfo;
import org.renci.binning.dao.BinningDAOBeanService;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.IncidentalBinX;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.dao.clinbin.model.IncidentalStatusType;
import org.renci.binning.incidental.uncseq.executor.IncidentalUNCSeqTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncidentalUNCSeqServiceImpl implements IncidentalUNCSeqService {

    private static final Logger logger = LoggerFactory.getLogger(IncidentalUNCSeqServiceImpl.class);

    private BinningDAOBeanService binningDAOBeanService;

    private BinningExecutorService binningExecutorService;

    public IncidentalUNCSeqServiceImpl() {
        super();
    }

    @Override
    public Response submit(IncidentalBinningJobInfo info) {
        logger.debug("ENTERING submit(IncidentalBinningJobInfo)");
        logger.info(info.toString());
        IncidentalBinningJob binningJob = new IncidentalBinningJob();
        try {
            binningJob.setStudy("UNCSeq Cancer Study");
            binningJob.setGender(info.getGender());
            binningJob.setParticipant(info.getParticipant());
            binningJob.setListVersion(info.getListVersion());
            binningJob.setStatus(binningDAOBeanService.getIncidentalStatusTypeDAO().findById("Requested"));
            IncidentalBinX incidentalBin = binningDAOBeanService.getIncidentalBinXDAO().findById(info.getIncidentalBinId());
            binningJob.setIncidentalBinX(incidentalBin);
            List<IncidentalBinningJob> foundBinningJobs = binningDAOBeanService.getIncidentalBinningJobDAO().findByExample(binningJob);
            if (CollectionUtils.isNotEmpty(foundBinningJobs)) {
                binningJob = foundBinningJobs.get(0);
            } else {
                binningJob.setId(binningDAOBeanService.getIncidentalBinningJobDAO().save(binningJob));
            }
            info.setId(binningJob.getId());
            logger.info(binningJob.toString());

            binningExecutorService.getExecutor().submit(new IncidentalUNCSeqTask(binningJob.getId()));

        } catch (BinningDAOException e) {
            logger.error(e.getMessage(), e);
            return Response.serverError().build();
        }
        return Response.ok(info).build();
    }

    @Override
    public IncidentalStatusType status(Integer binningJobId) {
        logger.debug("ENTERING status(Integer)");
        try {
            IncidentalBinningJob foundBinningJob = binningDAOBeanService.getIncidentalBinningJobDAO().findById(binningJobId);
            logger.info(foundBinningJob.toString());
            return foundBinningJob.getStatus();
        } catch (BinningDAOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public BinningExecutorService getBinningExecutorService() {
        return binningExecutorService;
    }

    public void setBinningExecutorService(BinningExecutorService binningExecutorService) {
        this.binningExecutorService = binningExecutorService;
    }

    public BinningDAOBeanService getBinningDAOBeanService() {
        return binningDAOBeanService;
    }

    public void setBinningDAOBeanService(BinningDAOBeanService binningDAOBeanService) {
        this.binningDAOBeanService = binningDAOBeanService;
    }

}
