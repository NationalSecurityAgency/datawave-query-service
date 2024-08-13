package datawave.microservice.query.result;

import datawave.microservice.query.storage.QueryStatus;

public class QueryStatusResponse {
    
    private final String queryId;
    private final String[] userDn;
    private final String queryLogic;
    private final String queryPool;
    private final QueryStatus.QUERY_STATE queryState;
    private final QueryStatus.CREATE_STAGE createStage;
    
    public QueryStatusResponse(QueryStatus queryStatus) {
        queryId = queryStatus.getQueryKey().getQueryId();
        userDn = queryStatus.getCurrentUser().getDNs();
        queryLogic = queryStatus.getQueryKey().getQueryLogic();
        queryPool = queryStatus.getQueryKey().getQueryPool();
        queryState = queryStatus.getQueryState();
        createStage = queryStatus.getCreateStage();
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    public String[] getUserDn() {
        return userDn;
    }
    
    public String getQueryLogic() {
        return queryLogic;
    }
    
    public String getQueryPool() {
        return queryPool;
    }
    
    public QueryStatus.QUERY_STATE getQueryState() {
        return queryState;
    }
    
    public QueryStatus.CREATE_STAGE getCreateStage() {
        return createStage;
    }
}
