package org.renci.binning.incidental.uncseq.commons;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.renci.binning.core.BinningException;
import org.renci.binning.core.IRODSUtils;
import org.renci.binning.core.incidental.AbstractLoadVCFCallable;
import org.renci.binning.dao.BinningDAOBeanService;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.dao.jpa.BinningDAOManager;
import org.renci.binning.dao.ref.model.GenomeRef;
import org.renci.binning.dao.ref.model.GenomeRefSeq;
import org.renci.binning.dao.var.model.LocatedVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import htsjdk.samtools.liftover.LiftOver;
import htsjdk.samtools.util.Interval;

public class LoadVCFCallable extends AbstractLoadVCFCallable {

    private static final Logger logger = LoggerFactory.getLogger(LoadVCFCallable.class);

    private String binningDirectory;

    public LoadVCFCallable(BinningDAOBeanService daoBean, IncidentalBinningJob binningJob, String binningDirectory) {
        super(daoBean, binningJob);
        this.binningDirectory = binningDirectory;
    }

    @Override
    public String getLabName() {
        return "LCCC";
    }

    @Override
    public String getLibraryName() {
        return "unknown";
    }

    @Override
    public String getStudyName() {
        return "UNCSeq";
    }

    @Override
    public LocatedVariant liftOver(LocatedVariant locatedVariant) throws BinningException {
        logger.debug("ENTERING liftOver(LocatedVariant)");
        LocatedVariant ret;
        try {
            File chainFile = new File(String.format("%s/liftOver", System.getProperty("karaf.data")), "hg19ToHg38.over.chain.gz");
            GenomeRef build38GenomeRef = getDaoBean().getGenomeRefDAO().findById(4);
            LiftOver liftOver = new LiftOver(chainFile);
            Interval interval = new Interval(String.format("chr%s", locatedVariant.getGenomeRefSeq().getContig()),
                    locatedVariant.getPosition(), locatedVariant.getEndPosition());
            Interval loInterval = liftOver.liftOver(interval);
            List<GenomeRefSeq> genomeRefSeqList = getDaoBean().getGenomeRefSeqDAO().findByRefIdAndContigAndSeqTypeAndAccessionPrefix(
                    build38GenomeRef.getId(), locatedVariant.getGenomeRefSeq().getContig(), "Chromosome", "NC_");

            if (CollectionUtils.isEmpty(genomeRefSeqList)) {
                throw new BinningException("GenomeRefSeq not found");
            }

            ret = new LocatedVariant(build38GenomeRef, genomeRefSeqList.get(0), loInterval.getStart(), loInterval.getEnd(),
                    locatedVariant.getVariantType(), locatedVariant.getRef(), locatedVariant.getSeq());
        } catch (Exception e) {
            throw new BinningException(e);
        }
        return ret;
    }

    @Override
    public Set<String> getExcludesFilter() {
        logger.debug("ENTERING getExcludesFilter()");
        Set<String> excludesFilter = new HashSet<>();
        return excludesFilter;
    }

    @Override
    public File getVCF(String participant) throws BinningException {
        logger.debug("ENTERING getVCF(String)");
        Map<String, String> avuMap = new HashMap<String, String>();
        avuMap.put("MaPSeqStudyName", "NC_GENES");
        avuMap.put("MaPSeqWorkflowName", "NCGenesBaseline");
        avuMap.put("MaPSeqJobName", "GATKApplyRecalibration");
        avuMap.put("MaPSeqMimeType", "TEXT_VCF");
        String irodsFile = IRODSUtils.findFile(participant, avuMap);
        String participantDir = String.format("%s/annotation/ncgenes/%s", binningDirectory, participant);
        File vcfFile = IRODSUtils.getFile(irodsFile, participantDir);
        return vcfFile;
    }

    public static void main(String[] args) {
        try {
            BinningDAOManager daoMgr = BinningDAOManager.getInstance();
            IncidentalBinningJob binningJob = daoMgr.getDAOBean().getIncidentalBinningJobDAO().findById(4218);
            LoadVCFCallable callable = new LoadVCFCallable(daoMgr.getDAOBean(), binningJob, "/tmp");
            callable.call();
        } catch (BinningDAOException | BinningException e) {
            e.printStackTrace();
        }
    }

}
