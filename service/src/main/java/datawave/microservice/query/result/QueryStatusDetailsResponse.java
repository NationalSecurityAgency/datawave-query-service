package datawave.microservice.query.result;

import java.util.Set;

import datawave.core.query.logic.QueryKey;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.query.exception.DatawaveErrorCode;

public class QueryStatusDetailsResponse {
    
    private final QueryKey queryKey;
    private final QueryStatus.QUERY_STATE queryState;
    private final QueryStatus.CREATE_STAGE createStage;
    private final DatawaveUserDetails currentUser;
    private final Query query;
    private final Set<String> calculatedAuths;
    private final String plan;
    private final long numResultsReturned;
    private final long numResultsConsumed;
    private final long numResultsGenerated;
    private final int activeNextCalls;
    private final int maxConcurrentNextCalls;
    private final long lastPageNumber;
    private final boolean allowLongRunningQueryEmptyPages;
    private final long nextCount;
    private final long seekCount;
    private final long queryStartMillis;
    private final long lastUsedMillis;
    private final long lastUpdatedMillis;
    private final DatawaveErrorCode errorCode;
    private final String failureMessage;
    private final String stackTrace;
    
    public QueryStatusDetailsResponse(QueryStatus queryStatus) {
        queryKey = queryStatus.getQueryKey();
        queryState = queryStatus.getQueryState();
        createStage = queryStatus.getCreateStage();
        currentUser = queryStatus.getCurrentUser();
        query = queryStatus.getQuery();
        calculatedAuths = queryStatus.getCalculatedAuths();
        plan = queryStatus.getPlan();
        numResultsReturned = queryStatus.getNumResultsReturned();
        numResultsConsumed = queryStatus.getNumResultsConsumed();
        numResultsGenerated = queryStatus.getNumResultsGenerated();
        activeNextCalls = queryStatus.getActiveNextCalls();
        maxConcurrentNextCalls = queryStatus.getMaxConcurrentNextCalls();
        lastPageNumber = queryStatus.getLastPageNumber();
        allowLongRunningQueryEmptyPages = queryStatus.isAllowLongRunningQueryEmptyPages();
        nextCount = queryStatus.getNextCount();
        seekCount = queryStatus.getSeekCount();
        queryStartMillis = queryStatus.getQueryStartMillis();
        lastUsedMillis = queryStatus.getLastUsedMillis();
        lastUpdatedMillis = queryStatus.getLastUpdatedMillis();
        errorCode = queryStatus.getErrorCode();
        failureMessage = queryStatus.getFailureMessage();
        stackTrace = queryStatus.getStackTrace();
    }
    
    public QueryKey getQueryKey() {
        return queryKey;
    }
    
    public QueryStatus.QUERY_STATE getQueryState() {
        return queryState;
    }
    
    public QueryStatus.CREATE_STAGE getCreateStage() {
        return createStage;
    }
    
    public DatawaveUserDetails getCurrentUser() {
        return currentUser;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public Set<String> getCalculatedAuths() {
        return calculatedAuths;
    }
    
    public String getPlan() {
        return plan;
    }
    
    public long getNumResultsReturned() {
        return numResultsReturned;
    }
    
    public long getNumResultsConsumed() {
        return numResultsConsumed;
    }
    
    public long getNumResultsGenerated() {
        return numResultsGenerated;
    }
    
    public int getActiveNextCalls() {
        return activeNextCalls;
    }
    
    public int getMaxConcurrentNextCalls() {
        return maxConcurrentNextCalls;
    }
    
    public long getLastPageNumber() {
        return lastPageNumber;
    }
    
    public boolean isAllowLongRunningQueryEmptyPages() {
        return allowLongRunningQueryEmptyPages;
    }
    
    public long getNextCount() {
        return nextCount;
    }
    
    public long getSeekCount() {
        return seekCount;
    }
    
    public long getQueryStartMillis() {
        return queryStartMillis;
    }
    
    public long getLastUsedMillis() {
        return lastUsedMillis;
    }
    
    public long getLastUpdatedMillis() {
        return lastUpdatedMillis;
    }
    
    public DatawaveErrorCode getErrorCode() {
        return errorCode;
    }
    
    public String getFailureMessage() {
        return failureMessage;
    }
    
    public String getStackTrace() {
        return stackTrace;
    }
}
