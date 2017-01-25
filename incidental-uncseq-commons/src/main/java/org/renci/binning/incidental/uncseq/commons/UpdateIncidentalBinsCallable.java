package org.renci.binning.incidental.uncseq.commons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.renci.binning.dao.BinningDAOException;
import org.renci.binning.dao.clinbin.model.BinResultsFinalIncidentalX;
import org.renci.binning.dao.clinbin.model.BinResultsFinalIncidentalXPK;
import org.renci.binning.dao.clinbin.model.CarrierStatus;
import org.renci.binning.dao.clinbin.model.HaplotypeX;
import org.renci.binning.dao.clinbin.model.IncidentalBinGeneX;
import org.renci.binning.dao.clinbin.model.IncidentalBinGroupVersionX;
import org.renci.binning.dao.clinbin.model.IncidentalBinHaplotypeX;
import org.renci.binning.dao.clinbin.model.IncidentalBinningJob;
import org.renci.binning.dao.clinbin.model.IncidentalResultVersionX;
import org.renci.binning.dao.clinbin.model.MaxFrequency;
import org.renci.binning.dao.clinbin.model.MaxFrequencyPK;
import org.renci.binning.dao.clinbin.model.NCGenesFrequencies;
import org.renci.binning.dao.clinbin.model.NCGenesFrequenciesPK;
import org.renci.binning.dao.dbsnp.model.SNPMappingAgg;
import org.renci.binning.dao.hgmd.model.HGMDLocatedVariant;
import org.renci.binning.dao.jpa.BinningDAOManager;
import org.renci.binning.dao.refseq.model.Variants_61_2;
import org.renci.binning.dao.var.model.AssemblyLocatedVariant;
import org.renci.binning.dao.var.model.AssemblyLocatedVariantPK;
import org.renci.binning.dao.var.model.AssemblyLocatedVariantQC;
import org.renci.binning.dao.var.model.AssemblyLocatedVariantQCPK;
import org.renci.binning.dao.var.model.LocatedVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateIncidentalBinsCallable implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(UpdateIncidentalBinsCallable.class);

    private static final BinningDAOManager daoMgr = BinningDAOManager.getInstance();

    private Map<String, Object> variables;

    public UpdateIncidentalBinsCallable(Map<String, Object> variables) {
        super();
        this.variables = variables;
    }

    @Override
    public Void call() throws BinningDAOException {
        logger.debug("ENTERING run()");

        Object o = variables.get("job");
        if (o != null && o instanceof IncidentalBinningJob) {
            IncidentalBinningJob incidentalBinningJob = (IncidentalBinningJob) o;
            logger.info(incidentalBinningJob.toString());

            final Double threshold = 0.05;

            IncidentalResultVersionX incidentalResultVersion = daoMgr.getDAOBean().getIncidentalResultVersionXDAO()
                    .findById(incidentalBinningJob.getListVersion());
            logger.info(incidentalResultVersion.toString());

            List<IncidentalBinGroupVersionX> incidentalBinGroupVersionXList = daoMgr.getDAOBean().getIncidentalBinGroupVersionXDAO()
                    .findByIncidentalBinIdAndGroupVersion(incidentalBinningJob.getIncidentalBinX().getId(),
                            incidentalResultVersion.getIbinGroupVersion());

            if (CollectionUtils.isEmpty(incidentalBinGroupVersionXList)) {
                logger.error("Could not find group list version");
                return null;
            }

            Integer incidentalBinVersion = incidentalBinGroupVersionXList.get(0).getKey().getIncidentalBinVersion();

            // List<String> zygosityModeList = Arrays.asList("AD", "AR", "CX", "XLD", "XLR", "V", "Y", "RISK");
            List<String> zygosityModeList = Arrays.asList("AD", "AR", "CX", "XLD", "XLR", "V", "Y");

            List<BinResultsFinalIncidentalX> results = new ArrayList<>();

            logger.info("finding q3 (bin2a)");
            List<IncidentalBinHaplotypeX> incidentalBinHaplotypeXList = daoMgr.getDAOBean().getIncidentalBinHaplotypeXDAO()
                    .findByIncidentalBinIdAndVersionAndAssemblyIdAndHGMDVersionAndZygosityMode(
                            incidentalBinningJob.getIncidentalBinX().getId(), incidentalBinVersion,
                            incidentalBinningJob.getAssembly().getId(), incidentalResultVersion.getHgmdVersion(), zygosityModeList);

            CarrierStatus carrierStatus = daoMgr.getDAOBean().getCarrierStatusDAO().findById(1);

            ExecutorService es = Executors.newFixedThreadPool(6);

            try {
                if (CollectionUtils.isNotEmpty(incidentalBinHaplotypeXList)) {
                    for (IncidentalBinHaplotypeX incidentalBinHaplotype : incidentalBinHaplotypeXList) {

                        es.submit(() -> {

                            try {
                                HaplotypeX haplotype = incidentalBinHaplotype.getHaplotype();
                                LocatedVariant locatedVariant = haplotype.getLocatedVariant();
                                List<Variants_61_2> variants = daoMgr.getDAOBean().getVariants_61_2_DAO()
                                        .findByLocatedVariantId(locatedVariant.getId());

                                if (CollectionUtils.isNotEmpty(variants)) {
                                    for (Variants_61_2 variant : variants) {
                                        BinResultsFinalIncidentalX binResultsFinalIncidentalX = buildResult(incidentalBinningJob,
                                                incidentalResultVersion, variant);
                                        binResultsFinalIncidentalX.setCarrierStatus(carrierStatus);
                                        results.add(binResultsFinalIncidentalX);
                                    }
                                }
                            } catch (BinningDAOException e) {
                                e.printStackTrace();
                            }

                        });

                    }
                }

                List<String> variantEffectList = Arrays.asList("nonsense", "splice-site", "frameshifting indel", "nonsense indel",
                        "boundary-crossing indel", "potential RNA-editing site");

                List<IncidentalBinGeneX> incidentalBinGenes = daoMgr.getDAOBean().getIncidentalBinGeneXDAO()
                        .findByIncidentalBinIdAndVersionAndZygosityModes(incidentalBinningJob.getIncidentalBinX().getId(),
                                incidentalBinVersion, zygosityModeList);

                if (CollectionUtils.isNotEmpty(incidentalBinGenes)) {
                    for (IncidentalBinGeneX incidentalGene : incidentalBinGenes) {

                        es.submit(() -> {

                            try {
                                logger.info("finding incidental q1 (hgmd)");
                                List<Variants_61_2> variants = daoMgr.getDAOBean().getVariants_61_2_DAO()
                                        .findByAssemblyIdAndSampleNameAndHGMDVersionAndMaxFrequencyThresholdAndGeneId(
                                                incidentalBinningJob.getAssembly().getId(), incidentalBinningJob.getParticipant(),
                                                incidentalResultVersion.getHgmdVersion(), threshold, incidentalGene.getGene().getId());
                                if (CollectionUtils.isNotEmpty(variants)) {
                                    for (Variants_61_2 variant : variants) {
                                        BinResultsFinalIncidentalX binResultsFinalIncidentalX = buildResult(incidentalBinningJob,
                                                incidentalResultVersion, variant);
                                        binResultsFinalIncidentalX.setCarrierStatus(carrierStatus);
                                        results.add(binResultsFinalIncidentalX);
                                    }
                                }

                            } catch (BinningDAOException e) {
                                e.printStackTrace();
                            }

                        });

                        es.submit(() -> {

                            try {

                                logger.info("finding incidental q2 (non-hgmd)");
                                List<Variants_61_2> variants = daoMgr.getDAOBean().getVariants_61_2_DAO()
                                        .findByHGMDVersionAndMaxFrequencyThresholdAndGeneIdAndVariantEffectList(
                                                incidentalResultVersion.getHgmdVersion(), threshold, incidentalGene.getGene().getId(),
                                                variantEffectList);
                                if (CollectionUtils.isNotEmpty(variants)) {
                                    for (Variants_61_2 variant : variants) {
                                        BinResultsFinalIncidentalX binResultsFinalIncidentalX = buildResult(incidentalBinningJob,
                                                incidentalResultVersion, variant);
                                        binResultsFinalIncidentalX.setCarrierStatus(carrierStatus);
                                        results.add(binResultsFinalIncidentalX);
                                    }
                                }
                            } catch (BinningDAOException e) {
                                e.printStackTrace();
                            }

                        });

                    }
                }

                es.shutdown();
                es.awaitTermination(1L, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            results.forEach(a -> logger.debug(a.toString()));

            // // calculate incidental bins
            // List<BinResultsFinalIncidentalX> binResultsFinalIncidentalList =
            // daoMgr.getDAOBean().getBinResultsFinalIncidentalXDAO()
            // .findByParticipantAndIncidentalBinIdAndResultVersionAndCarrierStatusId(incidentalBinningJob.getParticipant(),
            // incidentalBinningJob.getIncidentalBinX().getId(), incidentalBinningJob.getListVersion(), 1);
            //
            // if (CollectionUtils.isNotEmpty(binResultsFinalIncidentalList)) {
            // logger.warn("Already have requested results.");
            // return null;
            // }
            //
            // if (CollectionUtils.isNotEmpty(incidentalBinGroupVersionXList)) {
            // List<IncidentalBinGeneX> incidentalBinGeneXList = daoMgr.getDAOBean().getIncidentalBinGeneXDAO()
            // .findByIncidentalBinIdAndVersion(incidentalBinningJob.getIncidentalBinX().getId(),
            // incidentalBinGroupVersionXList.get(0).getKey().getIncidentalBinVersion());
            // if (CollectionUtils.isEmpty(incidentalBinGeneXList)) {
            // logger.warn("No genes in this bin");
            // return null;
            // }
            // }

        }
        return null;
    }

    private BinResultsFinalIncidentalX buildResult(IncidentalBinningJob incidentalBinningJob,
            IncidentalResultVersionX incidentalResultVersion, Variants_61_2 variant) throws BinningDAOException {

        MaxFrequency maxFrequency = daoMgr.getDAOBean().getMaxFrequencyDAO()
                .findById(new MaxFrequencyPK(variant.getLocatedVariant().getId(), incidentalResultVersion.getGen1000SnpVersion()));

        List<SNPMappingAgg> snpMappingAggList = daoMgr.getDAOBean().getSNPMappingAggDAO()
                .findByLocatedVariantId(variant.getLocatedVariant().getId());

        List<HGMDLocatedVariant> hgmdLocatedVariant = daoMgr.getDAOBean().getHGMDLocatedVariantDAO()
                .findByLocatedVariantId(variant.getLocatedVariant().getId());

        AssemblyLocatedVariant assemblyLocatedVariant = daoMgr.getDAOBean().getAssemblyLocatedVariantDAO()
                .findById(new AssemblyLocatedVariantPK(incidentalBinningJob.getAssembly().getId(), variant.getLocatedVariant().getId()));

        AssemblyLocatedVariantQC assemblyLocatedVariantQC = daoMgr.getDAOBean().getAssemblyLocatedVariantQCDAO()
                .findById(new AssemblyLocatedVariantQCPK(incidentalBinningJob.getAssembly().getId(), variant.getLocatedVariant().getId()));

        Integer maxVersion = daoMgr.getDAOBean().getNCGenesFrequenciesDAO().findMaxVersion();
        NCGenesFrequencies ncGenesFrequencies = daoMgr.getDAOBean().getNCGenesFrequenciesDAO()
                .findById(new NCGenesFrequenciesPK(variant.getLocatedVariant().getId(), maxVersion.toString()));

        BinResultsFinalIncidentalXPK key = new BinResultsFinalIncidentalXPK(incidentalBinningJob.getParticipant(),
                variant.getKey().getMapNumber(), incidentalResultVersion.getBinningResultVersion(),
                incidentalBinningJob.getIncidentalBinX().getId(), incidentalBinningJob.getAssembly().getId(),
                variant.getLocatedVariant().getId());

        BinResultsFinalIncidentalX binResultsFinalIncidentalX = new BinResultsFinalIncidentalX(key, variant, maxFrequency,
                CollectionUtils.isNotEmpty(hgmdLocatedVariant) ? hgmdLocatedVariant.get(0) : null, assemblyLocatedVariant,
                assemblyLocatedVariantQC, ncGenesFrequencies,
                CollectionUtils.isNotEmpty(snpMappingAggList) ? snpMappingAggList.get(0) : null);
        return binResultsFinalIncidentalX;
    }

    public static void main(String[] args) {
        try {
            IncidentalBinningJob incidentalBinningJob = daoMgr.getDAOBean().getIncidentalBinningJobDAO().findById(1530);
            Map<String, Object> variables = new HashMap<>();
            variables.put("job", incidentalBinningJob);
            UpdateIncidentalBinsCallable callable = new UpdateIncidentalBinsCallable(variables);
            Future<Void> future = Executors.newSingleThreadExecutor().submit(callable);
            future.get();
        } catch (BinningDAOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}
