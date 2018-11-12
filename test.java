package com.indexvc.data.service.impl;

import com.indexvc.data.mapper.AsCompanyMapper;
import com.indexvc.data.model.AsCompany;
import com.indexvc.data.model.AsCompanyExample;
import com.indexvc.data.model.ext.CommonConsts;
import com.indexvc.data.model.ext.CompanyStatus;
import com.indexvc.data.service.AsCompanyService;
import com.indexvc.data.service.AsDataSourceService;
import com.indexvc.data.service.AsProductService;
import com.indexvc.model.mq.ComAndProMQ;
import com.indexvc.model.mq.CompanyMQ;
import com.indexvc.util.DateUtil;
import com.indexvc.util.StringUtil;
import com.indexvc.util.UUIDUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;

@Service
public class AsCompanyServiceImpl implements AsCompanyService {
    private Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private AsCompanyMapper mapper;
    @Autowired
    private AsDataSourceService asDataSourceService;
    @Autowired
    private AsProductService asProductService;

    @Override
    public AsCompany saveEntity(AsCompany entity) {
        if(null==entity.getId()) {
            entity.setId(UUIDUtil.getUUID());
            entity.setCreateTime(new Date());
            entity.setUpdateTime(new Date());
            mapper.insert(entity);
        }else{
            AsCompanyExample example = new AsCompanyExample();
            example.createCriteria().andIdEqualTo(entity.getId());
            entity.setUpdateTime(new Date());
            mapper.updateByExampleSelective(entity, example);
        }
        return entity;
    }

    @Override
    public void saveByInorUnion(ComAndProMQ inorComAndPro) {
        CompanyMQ mqCompany = inorComAndPro.getCompany();
        AsCompany asCompany = saveByInorCompany(mqCompany);
        asProductService.saveByInorProduct(inorComAndPro.getProducts(), asCompany.getId());
    }

    @Override
    public AsCompany saveByInorCompany(CompanyMQ inorCompany) {
        AsCompany asCompany = getByInorIdAndCompanyName(inorCompany.getId(), inorCompany.getCompanyName());
        if(asCompany == null){
            asCompany = new AsCompany();
        }
        asCompany.setName(inorCompany.getName());
        asCompany.setCompanyName(inorCompany.getCompanyName());
        asCompany.setWebsite(inorCompany.getWebsite());
        asCompany.setLogo(inorCompany.getLogo());
        asCompany.setAlias(inorCompany.getNameKeyword());
        asCompany.setIndustry(inorCompany.getIndustry());
        asCompany.setSubIndustry(inorCompany.getSubIndustry());
        asCompany.setTags(inorCompany.getTags());
        asCompany.setPhase(inorCompany.getInvestmentPhase());
        try{
            asCompany.setBeginTime(null==inorCompany.getEstablishment()?null: DateUtil.formatStandardDate(inorCompany.getEstablishment()));
        }catch (Exception e){
            log.error("[f]convertByInorCompany: prase establishment to string error,inorCompany id={}",inorCompany.getId());
        }
        asCompany.setCountry(inorCompany.getCountry());
        asCompany.setCity(inorCompany.getCity());
        //TODO 公司地址保存
        //asCompany.setAddressId();
        // 公司状态mapping
        asCompany.setCompanyStatus(getMappingOperatingState(inorCompany.getOperatingStatus()));
        //asCompany.setFundStatus(inorCompany.get);
        //asCompany.getFinancingDemand(inorCompany.get)
        asCompany.setSummary(inorCompany.getSummary());
        asCompany.setUpdateTime(new Date());

        asCompany.setStatus(CommonConsts.STATUS_AVAILABLE);
        //是否删除
        if(inorCompany.getDelete()){
            asCompany.setStatus(CommonConsts.STATUS_DELETED);
        }

        if(StringUtil.isEmpty(asCompany.getId())) {
            asCompany.setSource(CommonConsts.SOURCE_ZHIZHU);
            asCompany.setId(UUIDUtil.getUUID());
            asCompany.setCreateTime(new Date());
            mapper.insert(asCompany);
        }else{
            mapper.updateByPrimaryKeySelective(asCompany);
        }
        asDataSourceService.saveDataSource(CommonConsts.DATA_TYPE_COMPANY, inorCompany.getId(), asCompany.getId());
        return asCompany;
    }

    /**
     * 根据指蛛公司id,或者公司名字查找指数通是否已存在
     * @param inorId
     * @param companyName
     * @return
     */
    @Override
    public AsCompany getByInorIdAndCompanyName(String inorId, String companyName) {
        String asCompanyId = asDataSourceService.getAsId(CommonConsts.DATA_TYPE_COMPANY,inorId);
        if(!StringUtil.isEmpty(asCompanyId)) {
            return mapper.selectByPrimaryKey(asCompanyId);
        }
        AsCompanyExample example = new AsCompanyExample();
        example.createCriteria().andCompanyNameEqualTo(companyName);
        List<AsCompany> companys = mapper.selectByExample(example);
        if(CollectionUtils.isEmpty(companys)){
            return null;
        }
        return companys.get(0);
    }


    /**
     * 更改公司状态（备用）
     * @param inorCompany
     * @return
     */
    @Override
    public void changeStatusByInorCompany(CompanyMQ inorCompany){
        if(StringUtil.isEmpty(inorCompany.getId()) || inorCompany.getDelete() == null ){
            return;
        }
        String asCompanyId = asDataSourceService.getAsId(CommonConsts.DATA_TYPE_COMPANY, inorCompany.getId());
        if(asCompanyId == null) {
            return ;
        }
        AsCompany asCompany = new AsCompany();
        if(inorCompany.getDelete()){
            asCompany.setId(asCompanyId);
            asCompany.setStatus(CommonConsts.STATUS_DELETED);
        }else {
            asCompany.setId(asCompanyId);
            asCompany.setStatus(CommonConsts.STATUS_AVAILABLE);
        }
        mapper.updateByPrimaryKeySelective(asCompany);
        //TODO 是否更改关联产品及融资记录的状态
    }

    /**
     * 公司入库时，mapping公司运营状态
     * @param operatingStatus
     * @return
     */
    private String getMappingOperatingState(String operatingStatus){
        if(StringUtil.isEmpty(operatingStatus)){
            return null;
        }
        String[] notOnlines = {"未上线"};
        String[] transformeds = {"已转型"};
        String[] closeds = {"吊销","已关闭","已注销","注销"};
        String[] operations = {"在业","在营","在营（开业）企业","存续","存续(在营、开业、在册)","存续（在营、开业、在册）","开业","迁出","运营中"};

        for(String str : notOnlines){
            if(operatingStatus.contains(str)){
                return CompanyStatus.未上线.name();
            }
        }
        for(String str : transformeds){
            if(operatingStatus.contains(str)){
                return CompanyStatus.已转型.name();
            }
        }
        for(String str : closeds){
            if(operatingStatus.contains(str)){
                return CompanyStatus.已关闭.name();
            }
        }
        for(String str : operations){
            if(operatingStatus.contains(str)){
                return CompanyStatus.运营中.name();
            }
        }
        return null;
    }
}
