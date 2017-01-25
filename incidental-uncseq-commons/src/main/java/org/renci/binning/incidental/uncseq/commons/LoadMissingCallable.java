package org.renci.binning.incidental.uncseq.commons;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.collections4.CollectionUtils;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.HaplotypeX;
import org.renci.binning.dao.clinbin.model.IncidentalBinGroupVersionX;
import org.renci.binning.dao.clinbin.model.IncidentalBinHaplotypeX;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.dao.clinbin.model.MissingHaplotype;
import org.renci.binning.dao.clinbin.model.MissingHaplotypePK;
import org.renci.binning.dao.jpa.BinningDAOManager;
import org.renci.binning.dao.var.model.LocatedVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;

public class LoadMissingCallable implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(LoadMissingCallable.class);

    private static final BinningDAOManager daoMgr = BinningDAOManager.getInstance();

    private Map<String, Object> variables;

    public LoadMissingCallable(Map<String, Object> variables) {
        super();
        this.variables = variables;
    }

    @Override
    public Void call() throws BinningDAOException {
        logger.debug("ENTERING run()");

        Object o = variables.get("job");
        if (o != null && o instanceof IncidentalBinningJob) {
            IncidentalBinningJob job = (IncidentalBinningJob) o;
            logger.info(job.toString());

            List<IncidentalBinGroupVersionX> incidentalBinGroupVersionXList = daoMgr.getDAOBean().getIncidentalBinGroupVersionXDAO()
                    .findByIncidentalBinIdAndGroupVersion(job.getIncidentalBinX().getId(), job.getListVersion());
            if (CollectionUtils.isEmpty(incidentalBinGroupVersionXList)) {
                return null;
            }

            List<MissingHaplotype> missingHaplotypeList = daoMgr.getDAOBean().getMissingHaplotypeDAO()
                    .findByParticipantAndIncidentalBinIdAndListVersion(job.getParticipant(), job.getIncidentalBinX().getId(),
                            job.getListVersion());
            if (CollectionUtils.isEmpty(missingHaplotypeList)) {
                return null;
            }

            List<IncidentalBinHaplotypeX> incidentalBinHaplotypeXList = daoMgr.getDAOBean().getIncidentalBinHaplotypeXDAO()
                    .findByIncidentalBinIdAndVersion(job.getIncidentalBinX().getId(),
                            missingHaplotypeList.get(0).getKey().getListVersion());

            if (CollectionUtils.isEmpty(incidentalBinHaplotypeXList)) {
                return null;
            }

            File vcfFile = new File(job.getVcfFile());
            try (VCFFileReader vcfFileReader = new VCFFileReader(vcfFile, false)) {
                for (IncidentalBinHaplotypeX incidentalBinHaplotypeX : incidentalBinHaplotypeXList) {
                    HaplotypeX haplotype = incidentalBinHaplotypeX.getHaplotype();
                    LocatedVariant locatedVariant = haplotype.getLocatedVariant();

                    for (VariantContext variantContext : vcfFileReader) {
                        if (!variantContext.hasGenotypes()) {
                            continue;
                        }
                        List<Genotype> genotypeList = variantContext.getGenotypes();
                        for (Genotype genotype : genotypeList) {
                            if (genotype.isNoCall()) {
                                continue;
                            }
                        }
                        if (locatedVariant.getGenomeRefSeq().getVerAccession().equals(variantContext.getContig())
                                && locatedVariant.getPosition().equals(variantContext.getStart())) {
                            MissingHaplotypePK key = new MissingHaplotypePK(job.getParticipant(), job.getIncidentalBinX().getId(),
                                    missingHaplotypeList.get(0).getKey().getListVersion(), locatedVariant.getId());
                            MissingHaplotype missingHaplotype = new MissingHaplotype(key);
                            daoMgr.getDAOBean().getMissingHaplotypeDAO().save(missingHaplotype);
                        }
                    }

                }

            }

        }
        return null;
    }

}
