package com.indexvc.inor.service.solr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.indexvc.core.Pagination;
import com.indexvc.dubbo.tool.vo.DictionaryEnum;
import com.indexvc.inor.dubbo.ToolDubboClient;
import com.indexvc.inor.model.Company;
import com.indexvc.inor.model.index.CompanyIndex;
import com.indexvc.inor.service.CompanyService;
import com.indexvc.inor.web.util.Pinyin4jUtil;
import com.indexvc.util.SolrUtil;
import com.indexvc.util.StringUtil;

@Service("companyIndexService")
public class CompanyIndexServiceImpl implements CompanyIndexService {

    @Autowired
    private ToolDubboClient toolDubboClient;
    @Autowired
    private CompanyService companyService;
    private String solrCore = "as_company";
    @Autowired
    private SolrClient solrClient;
    @Autowired
    private ProductIndexService productIndexService;

    @Override
    public void add(List<CompanyIndex> investors) {
        for (CompanyIndex entity : investors) {
            add(entity);
        }
    }

    @Override
    public void updateByDeltaImport() {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("command", "delta-import");
        params.set("clean", false);
        params.set("commit", true);
        QueryRequest request = new QueryRequest(params);
        request.setPath("/dataimport");
        try {
            solrClient.request(request, solrCore);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    @Override
    public void add(String companyId) {
        if (!StringUtil.isEmpty(companyId)) {
            add(companyService.loadForIndex(companyId));
        }
    }

    @Deprecated
    @Override
    public void add(CompanyIndex companyIndex) {
        companyIndex = companyIndex.handleSingleToList(companyIndex);
        if (!StringUtil.isEmpty(companyIndex.getCity())) {
            companyIndex.setCityPinyin(Pinyin4jUtil.converterToSpell(companyIndex.getCity()));
        }
        if (!StringUtil.isEmpty(companyIndex.getName())) {
            companyIndex.setNamePinyin(Pinyin4jUtil.converterToSpell(companyIndex.getName()));
        }
        try {
            delete(companyIndex.getId());
            UpdateResponse response = solrClient.addBean(solrCore, companyIndex);
            // 添加Index Bean到索引库
            solrClient.commit(solrCore);// commit后才保存到索引库
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void delete(List<Company> investorList) {
        for (Company entity : investorList) {
            delete(entity.getId());
        }
    }

    @Override
    public void delete(String id) {
        try {
            solrClient.deleteById(solrCore, id);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            solrClient.commit(solrCore);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteAll() {
        try {
            solrClient.deleteByQuery(solrCore, "*.*");
            solrClient.commit(solrCore);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
        }
    }

    @Override
    public void addAllByFullImport() {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("command", "full-import");
        QueryRequest request = new QueryRequest(params);
        request.setPath("/dataimport");
        try {
            solrClient.request(request, solrCore);
            System.out.println("full-import fail");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("full-import fail");

        }
    }

    @Override
    public Pagination<CompanyIndex> queryPageForHead() {
        Pagination<CompanyIndex> page = new Pagination<CompanyIndex>();
        CompanyIndex company = new CompanyIndex();
        page.setLength(0);
        SolrQuery query = handleQuery(page, company, null);
        FacetField facetFields = null;
        try {
            QueryResponse resp = solrClient.query(solrCore, query);
            facetFields = resp.getFacetField("industry");
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
        if (null != facetFields) {
            List<Count> fieldcount = facetFields.getValues();
            Collections.sort(fieldcount, new Comparator<Count>() {
                @Override
                public int compare(Count arg0, Count arg1) {
                    return arg0.getCount() > arg1.getCount() ? 1 : 0;
                }
            });
            List<String> tags =
                toolDubboClient.dictionaryDubboService.selectResource(DictionaryEnum.FocusIndustry.getCategory());

            for (Count obj : fieldcount) {
                if (obj.getName().length() > 1 && tags.contains(obj.getName())) {
                    page.addFacet(obj.getName(), Long.valueOf(obj.getCount()).intValue());
                }
            }
        }
        page.addBegin();
        return page;
    }

    @Override
    public Pagination<Company> queryPage(Pagination<CompanyIndex> page, CompanyIndex company, String type) {
        Pagination<Company> result = new Pagination<Company>();
        SolrQuery query = handleQuery(page, company, type);
        SolrDocumentList list = null;
        try {
            QueryResponse resp = solrClient.query(solrCore, query);
            list = resp.getResults();
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
        List solrBeanList = solrClient.getBinder().getBeans(CompanyIndex.class, list);
        result.setDataList(CompanyIndex.convertList(solrBeanList));
        Long totalResults = list.getNumFound();// 命中的总记录数
        result.setCount(totalResults.intValue());
        result.addBegin();
        return result;
    }

    @Override
    public Pagination<Company> queryPageAll(Pagination<CompanyIndex> page, CompanyIndex company) {
        page.addBegin();
        Pagination<Company> result = new Pagination<Company>();
        SolrQuery query = handleGlobalQuery(page, company);
        SolrDocumentList list = null;
        try {
            QueryResponse resp = solrClient.query(solrCore, query);
            list = resp.getResults();
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
        List solrBeanList = solrClient.getBinder().getBeans(CompanyIndex.class, list);
        result.setDataList(CompanyIndex.convertList(solrBeanList));
        Long totalResults = list.getNumFound();// 命中的总记录数
        result.setCount(totalResults.intValue());
        result.setBegin(page.getBegin());
        return result;
    }

    @Override
    public List<Company> querySimilarCompany(Pagination<CompanyIndex> page, CompanyIndex company) {
        SolrQuery query = handleGlobalQuery(page, company);
        SolrDocumentList list = null;
        try {
            QueryResponse resp = solrClient.query(solrCore, query);
            list = resp.getResults();
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
        List solrBeanList = solrClient.getBinder().getBeans(CompanyIndex.class, list);
        return null;
    }

    private SolrQuery handleQuery(Pagination<CompanyIndex> page, CompanyIndex companyIndex, String type) {
        String filterName = "指数资本测试";
        StringBuffer q = new StringBuffer("");
        if (StringUtil.isEmpty(page.getSearchKey())) {
            if (StringUtil.isEmpty(type)) {
                q.append("text:*");
            } else {
                q.append("nameWithLatelyInvestorg:*");
            }
        } else {
            if (StringUtil.isEmpty(type)) {
                q.append("\"" + page.getSearchKey() + "\"");
            } else {
                q.append(" nameWithLatelyInvestorg:\"" + page.getSearchKey() + "\"");
            }
        }
        SolrQuery query = new SolrQuery(q.toString());
        query = SolrUtil.generateFilter(query, companyIndex, "country,city,latelyInvestTime".split(","));
        companyIndex = companyIndex == null ? new CompanyIndex() : companyIndex;
        if (!StringUtil.isEmpty(companyIndex.getCity())) {
            StringBuffer params = new StringBuffer("(");

            if ("other".equals(companyIndex.getCity())) {
                String[] citys = {"北京", "上海", "广州", "杭州", "深圳"};
                params.append(StringUtil.listToString(Arrays.asList(citys), " AND ", "-(city:*", "*)"));
            } else {
                params.append(" city:(" + companyIndex.getCity().replace(",", " OR ") + ")");
            }
            // query.addFilterQuery(" city:("+ companyIndex.getCity().replace(",", " OR ")+")");
            if (!StringUtil.isEmpty(companyIndex.getCountry())) {
                params.append(" or country:(" + companyIndex.getCountry().replace(",", " OR ") + ")");
            }
            params.append(" )");
            query.addFilterQuery(params.toString());
        } else {
            if (!StringUtil.isEmpty(companyIndex.getCountry())) {
                query.addFilterQuery(" country:(" + companyIndex.getCountry().replace(",", " OR ") + ")");
            }
        }
        // 判断职位(主要模块搜索中排除包含”测试“的数据， 仅职位为技术的可看，比如项目服务、项目bd、联系。。。 投资人BD、待激活、已激活 、投资机构BD 激活等)
        if (page.getDisplayTest()) {
            query.addFilterQuery("-( companyName:\"" + filterName + "\") AND -( name:\"" + filterName + "\") ");
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        if (null != page.getStartDate() && null != page.getEndDate()) {
            query.addFilterQuery(
                "latelyInvestTime:[" + sdf.format(page.getStartDate()) + " TO " + sdf.format(page.getEndDate()) + "]");
        } else if (null != page.getStartDate()) {
            query.addFilterQuery("latelyInvestTime:[" + sdf.format(page.getStartDate()) + " TO *}");
        } else if (null != page.getEndDate()) {
            query.addFilterQuery("latelyInvestTime:{* TO " + sdf.format(page.getEndDate()) + "]");
        }

        query.setFacet(true).addFacetField("category1s");
        query.setStart(page.getBegin()).setRows(page.getLength());// 进行分页查询
        query.setHighlight(true).setHighlightSimplePre("<span color='red'>").setHighlightSimplePost("</span>");// 高亮
        if (StringUtil.isEmpty(page.getSearchKey())) {
            for (Pagination.Orders o : page.getOrders()) {
                if (StringUtil.isEmpty(o.getField())) {
                    continue;
                }
                if (o.getField().equals("name") || o.getField().equals("city")) {
                    o.setField(o.getField() + "Pinyin");
                }
                if (o.getField().equals("latelyInvestTime")) {
                    if ("asc".equals(o.getDir())) {
                        query.addSort("latelyInvestTime", ORDER.asc);
                    } else {
                        query.addSort("latelyInvestTime", ORDER.desc);
                    }
                    query.addSort("namePinyin", ORDER.asc);
                } else {
                    if ("asc".equals(o.getDir())) {
                        query.addSort(o.getField(), ORDER.asc);
                    } else if ("desc".equals(o.getDir())) {
                        query.addSort(o.getField(), ORDER.desc);
                    }
                }
            }
            if (null == page.getOrders() || page.getOrders().size() == 0) {
                query.addSort("latelyInvestTime", ORDER.desc);
                query.addSort("namePinyin", ORDER.asc);
            }
        }
        return query;
    }

    private SolrQuery handleGlobalQuery(Pagination<CompanyIndex> page, CompanyIndex company) {
        StringBuffer q = new StringBuffer("");
        if (StringUtil.isEmpty(page.getSearchKey())) {
            q.append("text:*");
        } else {
            q.append("text:" + page.getSearchKey());
        }
        SolrQuery query = new SolrQuery(q.toString());
        query.setStart(page.getBegin()).setRows(page.getLength());// 进行分页查询
        return query;
    }

    private SolrQuery handleQuery2(Pagination<Company> page, Company company) {
        CompanyIndex companyIndex = CompanyIndex.copyProperties(company);
        StringBuffer q = new StringBuffer("");
        if (StringUtil.isEmpty(page.getSearchKey())) {
            q.append("text:*");
        } else {
            q.append("text:" + page.getSearchKey());
        }
        SolrQuery query = new SolrQuery(q.toString());
        /*if (!StringUtil.isEmpty(companyIndex.getInvestmentZone())){
        	query.addFilterQuery(" investmentZone:"+ companyIndex.getInvestmentZone());
        }*/
        // todo 2017年2月17日 15:03:25 修改为根据country来判断是否是境内公司
        if (!StringUtil.isEmpty(companyIndex.getId())) {
            query.addFilterQuery(" id:" + companyIndex.getId());
        }
        if (!StringUtil.isEmpty(companyIndex.getCountry())) {
            query.addFilterQuery(" country:" + companyIndex.getCountry());
        } else {
            query.addFilterQuery(" country:['' TO *] AND -country:中国");
        }
        if (!StringUtil.isEmpty(companyIndex.getSource()) && companyIndex.getSource().equals("in")) {
            query.addFilterQuery(" -(source:*)");
        } else if (!StringUtil.isEmpty(companyIndex.getSource()) && !companyIndex.getSource().equals("in")) {
            query.addFilterQuery("  source:*");
        }

        query.setFacet(true).addFacetField("category1s");
        query.setStart(page.getBegin()).setRows(page.getLength());// 进行分页查询
        query.setHighlight(true).setHighlightSimplePre("<span color='red'>").setHighlightSimplePost("</span>");// 高亮
        if (StringUtil.isEmpty(page.getSearchKey())) {
            if (null == page.getOrders() || page.getOrders().size() == 0) {
                query.addSort("latelyInvestTime", ORDER.desc);
                query.addSort("topTotalOrgInvestNo", ORDER.desc);
            }
            for (Pagination.Orders o : page.getOrders()) {
                if (StringUtil.isEmpty(o.getField())) {
                    continue;
                }
                if (o.getField().equals("name") || o.getField().equals("city")) {
                    o.setField(o.getField() + "Pinyin");
                }
                if ("asc".equals(o.getDir())) {
                    query.addSort(o.getField(), ORDER.asc);
                } else {
                    query.addSort(o.getField(), ORDER.desc);
                }
            }
        }
        return query;
    }

}
