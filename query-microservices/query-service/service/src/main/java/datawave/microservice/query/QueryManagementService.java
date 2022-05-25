package datawave.microservice.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.marking.SecurityMarking;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.remote.QueryRequestHandler;
import datawave.microservice.query.runner.NextCall;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.util.QueryStatusUpdateUtil;
import datawave.microservice.query.util.QueryUtil;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.security.util.ProxiedEntityUtils;
import datawave.services.common.audit.PrivateAuditConstants;
import datawave.services.query.cache.ResultsPage;
import datawave.services.query.logic.QueryLogic;
import datawave.services.query.logic.QueryLogicFactory;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryCanceledQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.TimeoutQueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.logic.QueryLogicDescription;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.CANCELED;
import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.CLOSED;
import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.CREATED;
import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.DEFINED;
import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.PLANNED;
import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.PREDICTED;
import static datawave.webservice.query.QueryImpl.DN_LIST;
import static datawave.webservice.query.QueryImpl.QUERY_ID;
import static datawave.webservice.query.QueryImpl.USER_DN;

@Service
public class QueryManagementService implements QueryRequestHandler {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private final QueryProperties queryProperties;
    
    private final ApplicationEventPublisher eventPublisher;
    private final BusProperties busProperties;
    
    // Note: QueryParameters needs to be request scoped
    private final QueryParameters queryParameters;
    // Note: SecurityMarking needs to be request scoped
    private final SecurityMarking securityMarking;
    // Note: BaseQueryMetric needs to be request scoped
    private final BaseQueryMetric baseQueryMetric;
    
    private final QueryLogicFactory queryLogicFactory;
    private final QueryMetricClient queryMetricClient;
    private final ResponseObjectFactory responseObjectFactory;
    private final QueryStorageCache queryStorageCache;
    private final QueryResultsManager queryResultsManager;
    private final AuditClient auditClient;
    private final ThreadPoolTaskExecutor nextCallExecutor;
    
    private final QueryStatusUpdateUtil queryStatusUpdateUtil;
    private final MultiValueMap<String,NextCall> nextCallMap = new LinkedMultiValueMap<>();
    
    private final String selfDestination;
    
    private final Map<String,CountDownLatch> queryLatchMap = new ConcurrentHashMap<>();
    
    public QueryManagementService(QueryProperties queryProperties, ApplicationEventPublisher eventPublisher, BusProperties busProperties,
                    QueryParameters queryParameters, SecurityMarking securityMarking, BaseQueryMetric baseQueryMetric, QueryLogicFactory queryLogicFactory,
                    QueryMetricClient queryMetricClient, ResponseObjectFactory responseObjectFactory, QueryStorageCache queryStorageCache,
                    QueryResultsManager queryResultsManager, AuditClient auditClient, ThreadPoolTaskExecutor nextCallExecutor) {
        this.queryProperties = queryProperties;
        this.eventPublisher = eventPublisher;
        this.busProperties = busProperties;
        this.queryParameters = queryParameters;
        this.securityMarking = securityMarking;
        this.baseQueryMetric = baseQueryMetric;
        this.queryLogicFactory = queryLogicFactory;
        this.queryMetricClient = queryMetricClient;
        this.responseObjectFactory = responseObjectFactory;
        this.queryStorageCache = queryStorageCache;
        this.queryResultsManager = queryResultsManager;
        this.auditClient = auditClient;
        this.nextCallExecutor = nextCallExecutor;
        this.queryStatusUpdateUtil = new QueryStatusUpdateUtil(this.queryProperties, this.queryStorageCache);
        this.selfDestination = getSelfDestination();
    }
    
    /**
     * Gets a list of descriptions for the configured query logics, sorted by query logic name.
     * <p>
     * The descriptions include things like the audit type, optional and required parameters, required roles, and response class.
     *
     * @return the query logic descriptions
     */
    public QueryLogicResponse listQueryLogic(ProxiedUserDetails currentUser) {
        log.info("Request: listQueryLogic from {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        
        QueryLogicResponse response = new QueryLogicResponse();
        List<QueryLogic<?>> queryLogicList = queryLogicFactory.getQueryLogicList();
        List<QueryLogicDescription> logicConfigurationList = new ArrayList<>();
        
        // reference query necessary to avoid NPEs in getting the Transformer and BaseResponse
        Query q = new QueryImpl();
        Date now = new Date();
        q.setExpirationDate(now);
        q.setQuery("test");
        q.setQueryAuthorizations("ALL");
        
        for (QueryLogic<?> queryLogic : queryLogicList) {
            try {
                QueryLogicDescription logicDesc = new QueryLogicDescription(queryLogic.getLogicName());
                logicDesc.setAuditType(queryLogic.getAuditType(null).toString());
                logicDesc.setLogicDescription(queryLogic.getLogicDescription());
                
                Set<String> optionalQueryParameters = queryLogic.getOptionalQueryParameters();
                if (optionalQueryParameters != null) {
                    logicDesc.setSupportedParams(new ArrayList<>(optionalQueryParameters));
                }
                Set<String> requiredQueryParameters = queryLogic.getRequiredQueryParameters();
                if (requiredQueryParameters != null) {
                    logicDesc.setRequiredParams(new ArrayList<>(requiredQueryParameters));
                }
                Set<String> exampleQueries = queryLogic.getExampleQueries();
                if (exampleQueries != null) {
                    logicDesc.setExampleQueries(new ArrayList<>(exampleQueries));
                }
                Set<String> requiredRoles = queryLogic.getRequiredRoles();
                if (requiredRoles != null) {
                    List<String> requiredRolesList = new ArrayList<>(queryLogic.getRequiredRoles());
                    logicDesc.setRequiredRoles(requiredRolesList);
                }
                
                try {
                    logicDesc.setResponseClass(queryLogic.getResponseClass(q));
                } catch (QueryException e) {
                    log.error("Unable to get response class for query logic: {}", queryLogic.getLogicName(), e);
                    response.addException(e);
                    logicDesc.setResponseClass("unknown");
                }
                
                List<String> querySyntax = new ArrayList<>();
                try {
                    Method m = queryLogic.getClass().getMethod("getQuerySyntaxParsers");
                    Object result = m.invoke(queryLogic);
                    if (result instanceof Map<?,?>) {
                        Map<?,?> map = (Map<?,?>) result;
                        for (Object o : map.keySet())
                            querySyntax.add(o.toString());
                    }
                } catch (Exception e) {
                    log.warn("Unable to get query syntax for query logic: {}", queryLogic.getClass().getCanonicalName());
                }
                if (querySyntax.isEmpty()) {
                    querySyntax.add("CUSTOM");
                }
                logicDesc.setQuerySyntax(querySyntax);
                
                logicConfigurationList.add(logicDesc);
            } catch (Exception e) {
                log.error("Error setting query logic description", e);
            }
        }
        logicConfigurationList.sort(Comparator.comparing(QueryLogicDescription::getName));
        response.setQueryLogicList(logicConfigurationList);
        
        return response;
    }
    
    /**
     * Defines a query using the given query logic and parameters.
     * <p>
     * Defined queries cannot be started and run. <br>
     * Auditing is not performed when defining a query. <br>
     * Updates can be made to any parameter using {@link #update}. <br>
     * Create a runnable query from a defined query using {@link #duplicate} or {@link #reset}. <br>
     * Delete a defined query using {@link #remove}. <br>
     * Aside from a limited set of admin actions, only the query owner can act on a defined query.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a generic response containing the query id
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public GenericResponse<String> define(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/define from {} with params: {}", queryLogicName, user, parameters);
        } else {
            log.info("Request: {}/define from {}", queryLogicName, user);
        }
        
        try {
            TaskKey taskKey = storeQuery(queryLogicName, parameters, currentUser, DEFINED);
            GenericResponse<String> response = new GenericResponse<>();
            response.setResult(taskKey.getQueryId());
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error defining query", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error defining query.");
        }
    }
    
    /**
     * Creates a query using the given query logic and parameters.
     * <p>
     * Created queries will start running immediately. <br>
     * Auditing is performed before the query is started. <br>
     * Query results can be retrieved using {@link #next}.<br>
     * Updates can be made to any parameter which doesn't affect the scope of the query using {@link #update}. <br>
     * Stop a running query gracefully using {@link #close} or forcefully using {@link #cancel}. <br>
     * Stop, and restart a running query using {@link #reset}. <br>
     * Create a copy of a running query using {@link #duplicate}. <br>
     * Aside from a limited set of admin actions, only the query owner can act on a running query.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a generic response containing the query id
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public GenericResponse<String> create(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/create from {} with params: {}", queryLogicName, user, parameters);
        } else {
            log.info("Request: {}/create from {}", queryLogicName, user);
        }
        
        try {
            TaskKey taskKey = storeQuery(queryLogicName, parameters, currentUser, CREATED);
            GenericResponse<String> response = new GenericResponse<>();
            response.setResult(taskKey.getQueryId());
            response.setHasResults(true);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error creating query", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error creating query.");
        }
    }
    
    /**
     * Creates a query using the given query logic and parameters, but only for planning purposes.
     * <p>
     * Created queries will begin planning immediately. <br>
     * Auditing is performed if we are expanding indices. <br>
     * Query plan will be returned in the response.<br>
     * Updates can be made to any parameter which doesn't affect the scope of the query using {@link #update}. <br>
     * Stop a running query gracefully using {@link #close} or forcefully using {@link #cancel}. <br>
     * Stop, and restart a running query using {@link #reset}. <br>
     * Create a copy of a running query using {@link #duplicate}. <br>
     * Aside from a limited set of admin actions, only the query owner can act on a running query.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a generic response containing the query plan
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public GenericResponse<String> plan(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/plan from {} with params: {}", queryLogicName, user, parameters);
        } else {
            log.info("Request: {}/plan from {}", queryLogicName, user);
        }
        
        try {
            TaskKey taskKey = storeQuery(queryLogicName, parameters, currentUser, PLANNED);
            String queryPlan = queryStorageCache.getQueryStatus(taskKey.getQueryId()).getPlan();
            queryStorageCache.deleteQuery(taskKey.getQueryId());
            GenericResponse<String> response = new GenericResponse<>();
            response.setResult(queryPlan);
            response.setHasResults(true);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error planning query", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error planning query.");
        }
    }
    
    /**
     * Creates a query using the given query logic and parameters, but only for prediction purposes.
     * <p>
     * Created queries will begin predicting immediately. <br>
     * Auditing is not performed. <br>
     * Query prediction will be returned in the response.<br>
     * Updates can be made to any parameter which doesn't affect the scope of the query using {@link #update}. <br>
     * Stop a running query gracefully using {@link #close} or forcefully using {@link #cancel}. <br>
     * Stop, and restart a running query using {@link #reset}. <br>
     * Create a copy of a running query using {@link #duplicate}. <br>
     * Aside from a limited set of admin actions, only the query owner can act on a running query.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a generic response containing the query prediction
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public GenericResponse<String> predict(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/predict from {} with params: {}", queryLogicName, user, parameters);
        } else {
            log.info("Request: {}/predict from {}", queryLogicName, user);
        }
        
        try {
            TaskKey taskKey = storeQuery(queryLogicName, parameters, currentUser, PREDICTED);
            String queryPrediction = "This endpoint has not been implemented yet";
            queryStorageCache.deleteQuery(taskKey.getQueryId());
            GenericResponse<String> response = new GenericResponse<>();
            response.setResult(queryPrediction);
            response.setHasResults(true);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error predicting query", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error predicting query.");
        }
    }
    
    private TaskKey storeQuery(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser,
                    QueryStatus.QUERY_STATE queryType) throws QueryException {
        return storeQuery(queryLogicName, parameters, currentUser, queryType, null);
    }
    
    /**
     * Validates the query request, creates an entry in the query storage cache, and publishes a create event to the executor service.
     * <p>
     * Validation is run against the requested logic, the parameters, and the security markings in {@link #validateQuery}. <br>
     * Auditing is performed when {@code isCreateRequest} is <code>true</code> using {@link #audit}. <br>
     * If {@code queryId} is null, a query id will be generated automatically.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param currentUser
     *            the user who called this method, not null
     * @param queryType
     *            whether this is a define, create, or plan call
     * @param queryId
     *            the desired query id, may be null
     * @return the task key returned from query storage
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    private TaskKey storeQuery(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser,
                    QueryStatus.QUERY_STATE queryType, String queryId) throws BadRequestQueryException, QueryException {
        // validate query and get a query logic
        QueryLogic<?> queryLogic = validateQuery(queryLogicName, parameters, currentUser);
        
        String userId = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        log.trace("{} has authorizations {}", userId, currentUser.getPrimaryUser().getAuths());
        
        // set some audit parameters which are used internally
        String userDn = currentUser.getPrimaryUser().getDn().subjectDN();
        setInternalAuditParameters(queryLogicName, userDn, parameters);
        
        Query query = createQuery(queryLogicName, parameters, userDn, currentUser.getDNs(), queryId);
        
        // TODO: Downgrade the auths before or after auditing???
        // downgrade the auths
        Set<Authorizations> downgradedAuthorizations;
        try {
            downgradedAuthorizations = AuthorizationsUtil.getDowngradedAuthorizations(queryParameters.getAuths(), currentUser);
        } catch (Exception e) {
            throw new BadRequestQueryException("Unable to downgrade authorizations", e, HttpStatus.SC_BAD_REQUEST + "-1");
        }
        
        // if this is a create request, or a plan request where we are expanding values, send an audit record to the auditor
        if (queryType == CREATED || (queryType == PLANNED && queryParameters.isExpandValues())) {
            audit(query, queryLogic, parameters, currentUser);
        }
        
        try {
            // persist the query w/ query id in the query storage cache
            TaskKey taskKey = null;
            if (queryType == DEFINED) {
                // @formatter:off
                taskKey = queryStorageCache.defineQuery(
                        getPoolName(),
                        query,
                        downgradedAuthorizations,
                        getMaxConcurrentTasks(queryLogic));
                // @formatter:on
            } else if (queryType == CREATED) {
                // @formatter:off
                taskKey = queryStorageCache.createQuery(
                        getPoolName(),
                        query,
                        downgradedAuthorizations,
                        getMaxConcurrentTasks(queryLogic));
                // @formatter:on
                
                sendRequestAwaitResponse(QueryRequest.create(taskKey.getQueryId()), queryProperties.isAwaitExecutorCreateResponse());
            } else if (queryType == PLANNED) {
                // @formatter:off
                taskKey = queryStorageCache.planQuery(
                        getPoolName(),
                        query,
                        downgradedAuthorizations);
                // @formatter:on
                
                sendRequestAwaitResponse(QueryRequest.plan(taskKey.getQueryId()), true);
            } else if (queryType == PREDICTED) {
                // @formatter:off
                taskKey = queryStorageCache.predictQuery(
                        getPoolName(),
                        query,
                        downgradedAuthorizations);
                // @formatter:on
                
                sendRequestAwaitResponse(QueryRequest.predict(taskKey.getQueryId()), true);
            }
            
            if (taskKey == null) {
                log.error("Task Key not created for query");
                throw new QueryException(DatawaveErrorCode.RUNNING_QUERY_CACHE_ERROR);
            }
            
            // update the query metric
            if (queryType == DEFINED || queryType == CREATED) {
                baseQueryMetric.setQueryId(taskKey.getQueryId());
                baseQueryMetric.setLifecycle(BaseQueryMetric.Lifecycle.DEFINED);
                baseQueryMetric.populate(query);
                baseQueryMetric.setProxyServers(currentUser.getProxyServers());
            }
            
            return taskKey;
        } catch (Exception e) {
            log.error("Unknown error storing query", e);
            throw new QueryException(DatawaveErrorCode.RUNNING_QUERY_CACHE_ERROR, e);
        }
    }
    
    private void sendRequestAwaitResponse(QueryRequest request, boolean isAwaitResponse) throws QueryException {
        if (isAwaitResponse) {
            // before publishing the message, create a latch based on the query ID
            queryLatchMap.put(request.getQueryId(), new CountDownLatch(1));
        }
        
        // publish a plan event to the executor pool
        publishExecutorEvent(request, getPoolName());
        
        if (isAwaitResponse) {
            log.info("Waiting on query {} response from the executor.", request.getMethod().name());
            // wait for the executor to finish creating the query
            try {
                // TODO: Should we incorporate the call start time into this check?
                if (!queryLatchMap.get(request.getQueryId()).await(queryProperties.getExpiration().getCallTimeout(),
                                queryProperties.getExpiration().getCallTimeUnit())) {
                    throw new QueryException(DatawaveErrorCode.QUERY_TIMEOUT, "Timed out waiting for query " + request.getMethod().name() + " to finish.");
                } else {
                    log.info("Received query {} response from the executor.", request.getMethod().name());
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for query {} latch for queryId {}", request.getMethod().name(), request.getQueryId());
                throw new QueryException(DatawaveErrorCode.QUERY_TIMEOUT, "Interrupted while waiting for query " + request.getMethod().name() + " to finish.");
            } finally {
                queryLatchMap.remove(request.getQueryId());
            }
        }
    }
    
    /**
     * Creates a query using the given query logic and parameters, and returns the first page of results.
     * <p>
     * Created queries will start running immediately. <br>
     * Auditing is performed before the query is started. <br>
     * Subsequent query results can be retrieved using {@link #next}. <br>
     * Updates can be made to any parameter which doesn't affect the scope of the query using {@link #update}. <br>
     * Stop a running query gracefully using {@link #close} or forcefully using {@link #cancel}. <br>
     * Stop, and restart a running query using {@link #reset}. <br>
     * Create a copy of a running query using {@link #duplicate}. <br>
     * Aside from a limited set of admin actions, only the query owner can act on a running query.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a base query response containing the first page of results
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws TimeoutQueryException
     *             if the next call times out
     * @throws NoResultsQueryException
     *             if no query results are found
     * @throws QueryException
     *             if this next task is rejected by the executor
     * @throws QueryException
     *             if there is an unknown error
     */
    public BaseQueryResponse createAndNext(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/createAndNext from {} with params: {}", queryLogicName, user, parameters);
        } else {
            log.info("Request: {}/createAndNext from {}", queryLogicName, user);
        }
        
        String queryId = null;
        try {
            queryId = create(queryLogicName, parameters, currentUser).getResult();
            return next(queryId, currentUser.getPrimaryUser().getRoles());
        } catch (Exception e) {
            // TODO: QueryException results in VoidResponse. This differs from the original RestAPI where an exception was returned in a
            // DefaultEventQueryResponse.
            QueryException qe;
            if (!(e instanceof QueryException)) {
                log.error("Unknown error calling create and next. {}", queryId, e);
                qe = new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Unknown error calling create and next. " + queryId);
            } else {
                qe = (QueryException) e;
            }
            
            if (queryId != null && !(qe instanceof NoResultsQueryException)) {
                baseQueryMetric.setError(qe);
            }
            
            throw qe;
        }
    }
    
    /**
     * Gets the next page of results for the specified query.
     * <p>
     * Next can only be called on a running query. <br>
     * If configuration allows, multiple next calls may be run concurrently for a query. <br>
     * Only the query owner can call next on the specified query.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a base query response containing the next page of results
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws BadRequestQueryException
     *             if the query is not running
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the next call is interrupted
     * @throws TimeoutQueryException
     *             if the query times out
     * @throws NoResultsQueryException
     *             if no query results are found
     * @throws QueryException
     *             if this next task is rejected by the executor
     * @throws QueryException
     *             if next call execution fails
     * @throws QueryException
     *             if query logic creation fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public BaseQueryResponse next(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: next from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            // make sure the state is created
            if (queryStatus.getQueryState() == CREATED) {
                return next(queryId, currentUser.getPrimaryUser().getRoles());
            } else {
                throw new BadRequestQueryException("Cannot call next on a query that is not running", HttpStatus.SC_BAD_REQUEST + "-1");
            }
        } catch (Exception e) {
            // TODO: QueryException results in VoidResponse. This differs from the original RestAPI where an exception was returned in a
            // DefaultEventQueryResponse.
            QueryException qe;
            if (!(e instanceof QueryException)) {
                log.error("Unknown error getting next page for query {}", queryId, e);
                qe = new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Unknown error getting next page for query " + queryId);
            } else {
                qe = (QueryException) e;
            }
            
            if (!(qe instanceof NoResultsQueryException)) {
                baseQueryMetric.setError(qe);
            }
            
            throw qe;
        }
    }
    
    /**
     * Gets the next page of results for the given query, and publishes a next event to the executor service.
     * <p>
     * If configuration allows, multiple next calls may be run concurrently for a query.
     *
     * @param queryId
     *            the query id, not null
     * @param userRoles
     *            the roles of the user who called this method, not null
     * @return a base query response containing the next page of results
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws InterruptedException
     *             if the next call is interrupted
     * @throws TimeoutQueryException
     *             if the query times out
     * @throws NoResultsQueryException
     *             if no query results are found
     * @throws QueryException
     *             if this next task is rejected by the executor
     * @throws QueryException
     *             if next call execution fails
     * @throws QueryException
     *             if query logic creation fails
     */
    private BaseQueryResponse next(String queryId, Collection<String> userRoles) throws InterruptedException, QueryException {
        // before we spin up a separate thread, make sure we are allowed to call next
        boolean success = false;
        QueryStatus queryStatus = queryStatusUpdateUtil.lockedUpdate(queryId, queryStatusUpdateUtil::claimNextCall);
        try {
            // publish a next event to the executor pool
            publishNextEvent(queryId, queryStatus.getQueryKey().getQueryPool());
            
            // get the query logic
            String queryLogicName = queryStatus.getQuery().getQueryLogicName();
            QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(queryStatus.getQuery().getQueryLogicName(), userRoles);
            
            // update query metrics
            baseQueryMetric.setQueryId(queryId);
            baseQueryMetric.setQueryLogic(queryLogicName);
            
            // @formatter:off
            final NextCall nextCall = new NextCall.Builder()
                    .setQueryProperties(queryProperties)
                    .setResultsQueueManager(queryResultsManager)
                    .setQueryStorageCache(queryStorageCache)
                    .setQueryStatusUpdateUtil(queryStatusUpdateUtil)
                    .setQueryId(queryId)
                    .setQueryLogic(queryLogic)
                    .build();
            // @formatter:on
            
            nextCallMap.add(queryId, nextCall);
            try {
                // submit the next call to the executor
                nextCall.setFuture(nextCallExecutor.submit(nextCall));
                
                // wait for the results to be ready
                ResultsPage<Object> resultsPage = nextCall.getFuture().get();
                
                // update the query metric
                nextCall.updateQueryMetric(baseQueryMetric);
                
                // format the response
                if (!resultsPage.getResults().isEmpty()) {
                    BaseQueryResponse response = queryLogic.getTransformer(queryStatus.getQuery()).createResponse(resultsPage);
                    
                    // after all of our work is done, perform our final query status update for this next call
                    queryStatus = queryStatusUpdateUtil.lockedUpdate(queryId, status -> {
                        queryStatusUpdateUtil.releaseNextCall(status, queryResultsManager);
                        status.setLastPageNumber(status.getLastPageNumber() + 1);
                        status.setNumResultsReturned(status.getNumResultsReturned() + resultsPage.getResults().size());
                    });
                    success = true;
                    
                    response.setHasResults(true);
                    response.setPageNumber(queryStatus.getLastPageNumber());
                    response.setLogicName(queryLogicName);
                    response.setQueryId(queryId);
                    return response;
                } else {
                    if (nextCall.isCanceled()) {
                        log.debug("Query [{}]: Canceled while handling next call", queryId);
                        throw new QueryCanceledQueryException(DatawaveErrorCode.QUERY_CANCELED, MessageFormat.format("{0} canceled;", queryId));
                    } else if (baseQueryMetric.getLifecycle() == BaseQueryMetric.Lifecycle.NEXTTIMEOUT) {
                        log.debug("Query [{}]: Timed out during next call", queryId);
                        throw new TimeoutQueryException(DatawaveErrorCode.QUERY_TIMEOUT, MessageFormat.format("{0} timed out.", queryId));
                    } else {
                        log.debug("Query [{}]: No results found for next call - closing query", queryId);
                        // if there are no results, and we didn't timeout, close the query
                        close(queryId);
                        throw new NoResultsQueryException(DatawaveErrorCode.NO_QUERY_RESULTS_FOUND, MessageFormat.format("{0}", queryId));
                    }
                }
            } catch (TaskRejectedException e) {
                throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "Next task rejected by the executor for query " + queryId);
            } catch (ExecutionException e) {
                // try to unwrap the execution exception and throw a query exception
                throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e.getCause(), "Next call execution failed");
            } finally {
                // remove this next call from the map, and decrement the next count for this query
                nextCallMap.get(queryId).remove(nextCall);
            }
        } catch (CloneNotSupportedException e) {
            throw new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e,
                            "Unable to create instance of the requested query logic " + queryStatus.getQuery().getQueryLogicName());
        } finally {
            // update query status if we failed
            if (!success) {
                queryStatusUpdateUtil.lockedUpdate(queryId, status -> queryStatusUpdateUtil.releaseNextCall(status, queryResultsManager));
            }
        }
    }
    
    /**
     * Cancels the specified query.
     * <p>
     * Cancel can only be called on a running query, or a query that is in the process of closing. <br>
     * Outstanding next calls will be stopped immediately, but will return partial results if applicable. <br>
     * Aside from admins, only the query owner can cancel the specified query.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a void response indicating that the query was canceled
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws BadRequestQueryException
     *             if the query is not running
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the cancel call is interrupted
     * @throws QueryException
     *             if there is an unknown error
     */
    public VoidResponse cancel(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: cancel from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        return cancel(queryId, currentUser, false);
    }
    
    /**
     * Cancels the specified query using admin privileges.
     * <p>
     * Cancel can only be called on a running query, or a query that is in the process of closing. <br>
     * Outstanding next calls will be stopped immediately, but will return partial results if applicable. <br>
     * Only admin users should be allowed to call this method.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a void response indicating that the query was canceled
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws BadRequestQueryException
     *             if the query is not running
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the cancel call is interrupted
     * @throws QueryException
     *             if there is an unknown error
     */
    public VoidResponse adminCancel(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: adminCancel from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        return cancel(queryId, currentUser, true);
    }
    
    /**
     * Cancels all queries using admin privileges.
     * <p>
     * Cancel can only be called on a running query, or a query that is in the process of closing. <br>
     * Queries that are not running will be ignored by this method. <br>
     * Outstanding next calls will be stopped immediately, but will return partial results if applicable. <br>
     * Only admin users should be allowed to call this method.
     *
     * @param currentUser
     *            the user who called this method, not null
     * @return a void response specifying which queries were canceled
     * @throws NotFoundQueryException
     *             if a query cannot be found
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the cancel call is interrupted
     * @throws QueryException
     *             if there is an unknown error
     */
    public VoidResponse adminCancelAll(ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: adminCancelAll from {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        
        try {
            List<QueryStatus> queryStatuses = queryStorageCache.getQueryStatus();
            queryStatuses.removeIf(s -> s.getQueryState() != CREATED);
            
            VoidResponse response = new VoidResponse();
            for (QueryStatus queryStatus : queryStatuses) {
                cancel(queryStatus.getQueryKey().getQueryId(), true);
                response.addMessage(queryStatus.getQueryKey().getQueryId() + " canceled.");
            }
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, "Error encountered while canceling all queries.");
            log.error("Error encountered while canceling all queries", queryException);
            throw queryException;
        }
    }
    
    /**
     * Cancels the specified query.
     * <p>
     * Cancel can only be called on a running query, or a query that is in the process of closing. <br>
     * Outstanding next calls will be stopped immediately, but will return partial results if applicable. <br>
     * Publishes a cancel event to the query and executor services. <br>
     * Query ownership will only be validated when {@code adminOverride} is set to <code>true</code>.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @param adminOverride
     *            whether or not this is an admin action
     * @return a void response indicating that the query was canceled
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws BadRequestQueryException
     *             if the query is not running
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the cancel call is interrupted
     * @throws QueryException
     *             if there is an unknown error
     */
    private VoidResponse cancel(String queryId, ProxiedUserDetails currentUser, boolean adminOverride) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser, adminOverride);
            
            // if the query is running, or if the query is closing and finishing up a next call
            if (queryStatus.isRunning()) {
                cancel(queryId, true);
            } else {
                throw new BadRequestQueryException("Cannot call cancel on a query that is not running", HttpStatus.SC_BAD_REQUEST + "-1");
            }
            
            VoidResponse response = new VoidResponse();
            response.addMessage(queryId + " canceled.");
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, "Unknown error canceling query " + queryId);
            log.error("Unknown error canceling query {}", queryId, queryException);
            throw queryException;
        }
    }
    
    /**
     * Cancels the specified query, and optionally publishes a cancel event to the query and executor services.
     * <p>
     * Cancels any locally-running next calls. <br>
     * Called with {@code publishEvent} set to <code>true</code> when the user calls cancel. <br>
     * When {@code publishEvent} is <code>true</code>, changes the query state to {@link QueryStatus.QUERY_STATE#CANCELED}. <br>
     * Called with {@code publishEvent} set to <code>false</code> when handling a remote cancel event.
     *
     * @param queryId
     *            the query id, not null
     * @param publishEvent
     *            whether or not to publish an event
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws InterruptedException
     *             if the cancel call is interrupted
     */
    public void cancel(String queryId, boolean publishEvent) throws InterruptedException, QueryException {
        // if we have an active next call for this query locally, cancel it
        List<NextCall> nextCalls = nextCallMap.get(queryId);
        if (nextCalls != null) {
            nextCalls.forEach(NextCall::cancel);
        }
        
        if (publishEvent) {
            // only the initial event publisher should update the status
            QueryStatus queryStatus = queryStatusUpdateUtil.lockedUpdate(queryId, status -> {
                // update query state to CANCELED
                status.setQueryState(CANCELED);
            });
            
            // delete the results queue
            queryResultsManager.deleteQuery(queryId);
            
            QueryRequest cancelRequest = QueryRequest.cancel(queryId);
            
            // publish a cancel event to all of the query services
            publishSelfEvent(cancelRequest);
            
            // publish a cancel event to the executor pool
            publishExecutorEvent(cancelRequest, queryStatus.getQueryKey().getQueryPool());
            
            // update query metrics
            baseQueryMetric.setQueryId(queryId);
            baseQueryMetric.setLifecycle(BaseQueryMetric.Lifecycle.CANCELLED);
            baseQueryMetric.setLastUpdated(new Date());
            try {
                // @formatter:off
                queryMetricClient.submit(
                        new QueryMetricClient.Request.Builder()
                                .withMetric(baseQueryMetric.duplicate())
                                .withMetricType(QueryMetricType.DISTRIBUTED)
                                .build());
                // @formatter:on
            } catch (Exception e) {
                log.error("Error updating query metric", e);
            }
        }
    }
    
    /**
     * Closes the specified query.
     * <p>
     * Close can only be called on a running query. <br>
     * Outstanding next calls will be allowed to run until they can return a full page, or they timeout. <br>
     * Aside from admins, only the query owner can close the specified query.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a void response indicating that the query was closed
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws BadRequestQueryException
     *             if the query is not running
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the cancel call is interrupted
     * @throws QueryException
     *             if there is an unknown error
     */
    public VoidResponse close(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: close from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        return close(queryId, currentUser, false);
    }
    
    /**
     * Closes the specified query using admin privileges.
     * <p>
     * Close can only be called on a running query. <br>
     * Outstanding next calls will be allowed to run until they can return a full page, or they timeout. <br>
     * Only admin users should be allowed to call this method.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a void response indicating that the query was closed
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws BadRequestQueryException
     *             if the query is not running
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the cancel call is interrupted
     * @throws QueryException
     *             if there is an unknown error
     */
    public VoidResponse adminClose(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: adminClose from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        return close(queryId, currentUser, true);
    }
    
    /**
     * Closes all queries using admin privileges.
     * <p>
     * Close can only be called on a running query. <br>
     * Queries that are not running will be ignored by this method. <br>
     * Outstanding next calls will be allowed to run until they can return a full page, or they timeout. <br>
     * Only admin users should be allowed to call this method.
     *
     * @param currentUser
     *            the user who called this method, not null
     * @return a void response specifying which queries were closed
     * @throws NotFoundQueryException
     *             if a query cannot be found
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the cancel call is interrupted
     * @throws QueryException
     *             if there is an unknown error
     */
    public VoidResponse adminCloseAll(ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: adminCloseAll from {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        
        try {
            List<QueryStatus> queryStatuses = queryStorageCache.getQueryStatus();
            queryStatuses.removeIf(s -> s.getQueryState() != CREATED);
            
            VoidResponse response = new VoidResponse();
            for (QueryStatus queryStatus : queryStatuses) {
                close(queryStatus.getQueryKey().getQueryId());
                response.addMessage(queryStatus.getQueryKey().getQueryId() + " closed.");
            }
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.QUERY_CLOSE_ERROR, e, "Error encountered while closing all queries.");
            log.error("Error encountered while closing all queries", queryException);
            throw queryException;
        }
    }
    
    /**
     * Closes the specified query.
     * <p>
     * Close can only be called on a running query. <br>
     * Outstanding next calls will be allowed to run until they can return a full page, or they timeout. <br>
     * Query ownership will only be validated when {@code adminOverride} is set to <code>true</code>.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @param adminOverride
     *            whether or not this is an admin action
     * @return a void response indicating that the query was closed
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws BadRequestQueryException
     *             if the query is not running
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the cancel call is interrupted
     * @throws QueryException
     *             if there is an unknown error
     */
    private VoidResponse close(String queryId, ProxiedUserDetails currentUser, boolean adminOverride) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser, adminOverride);
            
            if (queryStatus.getQueryState() == CREATED) {
                close(queryId);
            } else {
                throw new BadRequestQueryException("Cannot call close on a query that is not running", HttpStatus.SC_BAD_REQUEST + "-1");
            }
            
            VoidResponse response = new VoidResponse();
            response.addMessage(queryId + " closed.");
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.QUERY_CLOSE_ERROR, e, "Unknown error closing query " + queryId);
            log.error("Unknown error closing query {}", queryId, queryException);
            throw queryException;
        }
    }
    
    /**
     * Closes the specified query, and publishes a close event to the executor services.
     * <p>
     * Changes the query state to {@link QueryStatus.QUERY_STATE#CLOSED}.
     *
     * @param queryId
     *            the query id, not null
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws InterruptedException
     *             if the cancel call is interrupted
     */
    public void close(String queryId) throws InterruptedException, QueryException {
        QueryStatus queryStatus = queryStatusUpdateUtil.lockedUpdate(queryId, status -> {
            // update query state to CLOSED
            status.setQueryState(CLOSED);
        });
        
        // if the query has no active next calls, delete the results queue
        if (queryStatus.getActiveNextCalls() == 0) {
            queryResultsManager.deleteQuery(queryId);
        }
        
        // publish a close event to the executor pool
        publishExecutorEvent(QueryRequest.close(queryId), queryStatus.getQueryKey().getQueryPool());
        
        // update query metrics
        baseQueryMetric.setQueryId(queryId);
        baseQueryMetric.setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        baseQueryMetric.setLastUpdated(new Date());
        try {
            // @formatter:off
            queryMetricClient.submit(
                    new QueryMetricClient.Request.Builder()
                            .withMetric(baseQueryMetric.duplicate())
                            .withMetricType(QueryMetricType.DISTRIBUTED)
                            .build());
            // @formatter:on
        } catch (Exception e) {
            log.error("Error updating query metric", e);
        }
    }
    
    /**
     * Stops, and restarts the specified query.
     * <p>
     * Reset can be called on any query, whether it's running or not. <br>
     * If the specified query is still running, it will be canceled. See {@link #cancel}. <br>
     * Reset creates a new, identical query, with a new query id. <br>
     * Reset queries will start running immediately. <br>
     * Auditing is performed before the new query is started.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a generic response containing the new query id
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the cancel call is interrupted
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public GenericResponse<String> reset(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: reset from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            // cancel the query if it is running
            if (queryStatus.isRunning()) {
                cancel(queryStatus.getQueryKey().getQueryId(), true);
            }
            
            // create a new query which is an exact copy of the specified query
            TaskKey taskKey = duplicate(queryStatus, new LinkedMultiValueMap<>(), currentUser);
            
            GenericResponse<String> response = new GenericResponse<>();
            response.addMessage(queryId + " reset.");
            response.setResult(taskKey.getQueryId());
            response.setHasResults(true);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error resetting query {}", queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_RESET_ERROR, e, "Unknown error resetting query " + queryId);
        }
    }
    
    /**
     * Removes the specified query from query storage.
     * <p>
     * Remove can only be called on a query that is not running. <br>
     * Aside from admins, only the query owner can remove the specified query.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a void response indicating that the query was removed
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws BadRequestQueryException
     *             if the query is running
     * @throws QueryException
     *             if there is an unknown error
     */
    public VoidResponse remove(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: remove from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        return remove(queryId, currentUser, false);
    }
    
    /**
     * Removes the specified query from query storage using admin privileges.
     * <p>
     * Remove can only be called on a query that is not running. <br>
     * Only admin users should be allowed to call this method.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a void response indicating that the query was removed
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws BadRequestQueryException
     *             if the query is running
     * @throws QueryException
     *             if there is an unknown error
     */
    public VoidResponse adminRemove(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: adminRemove from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        return remove(queryId, currentUser, true);
    }
    
    /**
     * Removes all queries from query storage using admin privileges.
     * <p>
     * Remove can only be called on a query that is not running. <br>
     * Queries that are running will be ignored by this method. <br>
     * Only admin users should be allowed to call this method.
     *
     * @param currentUser
     *            the user who called this method, not null
     * @return a void response specifying which queries were removed
     * @throws NotFoundQueryException
     *             if a query cannot be found
     * @throws QueryException
     *             if there is an unknown error
     */
    public VoidResponse adminRemoveAll(ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: adminRemoveAll from {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        
        try {
            List<QueryStatus> queryStatuses = queryStorageCache.getQueryStatus();
            queryStatuses.removeIf(QueryStatus::isRunning);
            
            VoidResponse response = new VoidResponse();
            for (QueryStatus queryStatus : queryStatuses) {
                if (remove(queryStatus)) {
                    response.addMessage(queryStatus.getQueryKey().getQueryId() + " removed.");
                }
            }
            return response;
        } catch (Exception e) {
            QueryException queryException = new QueryException(DatawaveErrorCode.QUERY_REMOVAL_ERROR, e, "Error encountered while removing all queries.");
            log.error("Error encountered while removing all queries", queryException);
            throw queryException;
        }
    }
    
    /**
     * Removes the specified query from query storage.
     * <p>
     * Remove can only be called on a query that is not running. <br>
     * Query ownership will only be validated when {@code adminOverride} is set to <code>true</code>.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @param adminOverride
     *            whether or not this is an admin action
     * @return a void response indicating that the query was removed
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws BadRequestQueryException
     *             if the query is running
     * @throws QueryException
     *             if there is an unknown error
     */
    private VoidResponse remove(String queryId, ProxiedUserDetails currentUser, boolean adminOverride) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser, adminOverride);
            
            // remove the query if it is not running
            if (!queryStatus.isRunning()) {
                if (!remove(queryStatus)) {
                    throw new QueryException("Failed to remove " + queryId);
                }
            } else {
                throw new BadRequestQueryException("Cannot remove a running query.", HttpStatus.SC_BAD_REQUEST + "-1");
            }
            
            VoidResponse response = new VoidResponse();
            response.addMessage(queryId + " removed.");
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error removing query {}", queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_REMOVAL_ERROR, e, "Unknown error removing query " + queryId);
        }
    }
    
    private boolean remove(QueryStatus queryStatus) throws IOException {
        return queryStorageCache.deleteQuery(queryStatus.getQueryKey().getQueryId());
    }
    
    /**
     * Updates the specified query.
     * <p>
     * Update can only be called on a defined, or running query. <br>
     * Auditing is not performed when updating a defined query. <br>
     * No auditable parameters should be updated when updating a running query. <br>
     * Any query parameter can be updated for a defined query. <br>
     * Query parameters which don't affect the scope of the query can be updated for a running query. <br>
     * The list of parameters that can be updated for a running query is configurable. <br>
     * Auditable parameters should never be added to the updatable parameters configuration. <br>
     * Query string, date range, query logic, and auths should never be updated for a running query. <br>
     * Only the query owner can call update on the specified query.
     *
     * @param queryId
     *            the query id, not null
     * @param parameters
     *            the query parameter updates, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a generic response containing the query id
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if the query is not defined, or running
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if the update call is interrupted
     * @throws BadRequestQueryException
     *             if no parameters are specified
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public GenericResponse<String> update(String queryId, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/update from {} with params: {}", queryId, user, parameters);
        } else {
            log.info("Request: {}/update from {}", queryId, user);
        }
        
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            GenericResponse<String> response = new GenericResponse<>();
            if (!parameters.isEmpty()) {
                // recreate the query parameters
                MultiValueMap<String,String> currentParams = new LinkedMultiValueMap<>();
                currentParams.addAll(queryStatus.getQuery().toMap());
                
                // remove some of the copied params
                currentParams.remove(QUERY_ID);
                currentParams.remove(USER_DN);
                currentParams.remove(DN_LIST);
                
                boolean updated;
                if (queryStatus.getQueryState() == DEFINED) {
                    // update all parameters if the state is defined
                    updated = updateParameters(parameters, currentParams);
                    
                    // redefine the query
                    if (updated) {
                        storeQuery(currentParams.getFirst(QUERY_LOGIC_NAME), currentParams, currentUser, DEFINED, queryId);
                    }
                } else if (queryStatus.isRunning()) {
                    // if the query is created/running, update safe parameters only
                    List<String> unsafeParams = new ArrayList<>(parameters.keySet());
                    List<String> safeParams = new ArrayList<>(queryProperties.getUpdatableParams());
                    safeParams.retainAll(parameters.keySet());
                    unsafeParams.removeAll(safeParams);
                    
                    // only update a running query if the params are all safe
                    if (unsafeParams.isEmpty()) {
                        updated = updateParameters(safeParams, parameters, currentParams);
                        
                        if (updated) {
                            // validate the update
                            String queryLogicName = currentParams.getFirst(QUERY_LOGIC_NAME);
                            validateQuery(queryLogicName, currentParams, currentUser);
                            
                            // create a new query object
                            String userDn = currentUser.getPrimaryUser().getDn().subjectDN();
                            Query query = createQuery(queryLogicName, currentParams, userDn, currentUser.getDNs(), queryId);
                            
                            // save the new query object in the cache
                            queryStatusUpdateUtil.lockedUpdate(queryId, status -> status.setQuery(query));
                        }
                    } else {
                        throw new BadRequestQueryException("Cannot update the following parameters for a running query: " + String.join(", ", unsafeParams),
                                        HttpStatus.SC_BAD_REQUEST + "-1");
                    }
                } else {
                    throw new BadRequestQueryException("Cannot update a query unless it is defined or running.", HttpStatus.SC_BAD_REQUEST + "-1");
                }
                
                response.setResult(queryId);
                if (updated) {
                    response.addMessage(queryId + " updated.");
                } else {
                    response.addMessage(queryId + " unchanged.");
                }
            } else {
                throw new BadRequestQueryException("No parameters specified for update.", HttpStatus.SC_BAD_REQUEST + "-1");
            }
            
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error updating query {}", queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_UPDATE_ERROR, e, "Unknown error updating query " + queryId);
        }
    }
    
    /**
     * Creates a copy of the specified query.
     * <p>
     * Duplicate can be called on any query, whether it's running or not. <br>
     * Duplicate creates a new, identical query, with a new query id. <br>
     * Provided parameter updates will be applied to the new query. <br>
     * Duplicated queries will start running immediately. <br>
     * Auditing is performed before the new query is started.
     *
     * @param queryId
     *            the query id, not null
     * @param parameters
     *            the query parameter updates, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a generic response containing the new query id
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public GenericResponse<String> duplicate(String queryId, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/duplicate from {} with params: {}", queryId, user, parameters);
        } else {
            log.info("Request: {}/duplicate from {}", queryId, user);
        }
        
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            // define a duplicate query from the existing query
            TaskKey taskKey = duplicate(queryStatus, parameters, currentUser);
            
            GenericResponse<String> response = new GenericResponse<>();
            response.addMessage(queryId + " duplicated.");
            response.setResult(taskKey.getQueryId());
            response.setHasResults(true);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error duplicating query {}", queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_DUPLICATION_ERROR, e, "Unknown error duplicating query " + queryId);
        }
    }
    
    /**
     * Creates a copy of the specified query.
     * <p>
     * Duplicate can be called on any query, whether it's running or not. <br>
     * Duplicate creates a new, identical query, with a new query id. <br>
     * Provided parameter updates will be applied to the new query. <br>
     * Duplicated queries will start running immediately. <br>
     * Auditing is performed before the new query is started.
     *
     * @param queryStatus
     *            the query status, not null
     * @param parameters
     *            the query parameter updates, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return a generic response containing the new query id
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     */
    private TaskKey duplicate(QueryStatus queryStatus, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        // recreate the query parameters
        MultiValueMap<String,String> currentParams = new LinkedMultiValueMap<>();
        currentParams.addAll(queryStatus.getQuery().toMap());
        
        // remove some of the copied params
        currentParams.remove(QUERY_ID);
        currentParams.remove(USER_DN);
        currentParams.remove(DN_LIST);
        
        // updated all of the passed in parameters
        updateParameters(parameters, currentParams);
        
        // define a duplicate query
        return storeQuery(currentParams.getFirst(QUERY_LOGIC_NAME), currentParams, currentUser, CREATED);
    }
    
    private boolean updateParameters(MultiValueMap<String,String> newParameters, MultiValueMap<String,String> currentParams) throws BadRequestQueryException {
        return updateParameters(new ArrayList<>(newParameters.keySet()), newParameters, currentParams);
    }
    
    /**
     * Updates the current parameters with the new parameters, limited to the specified parameter names.
     *
     * @param parameterNames
     *            the parameters names to override, not null
     * @param newParameters
     *            the new parameters, may be null
     * @param currentParams
     *            the current parameters, not null
     * @return true is the current parameters were changed
     * @throws BadRequestQueryException
     *             if a parameter is passed without a value
     */
    private boolean updateParameters(Collection<String> parameterNames, MultiValueMap<String,String> newParameters, MultiValueMap<String,String> currentParams)
                    throws BadRequestQueryException {
        boolean paramsUpdated = false;
        for (String paramName : parameterNames) {
            if (newParameters.get(paramName) != null && !newParameters.get(paramName).isEmpty()) {
                if (!newParameters.get(paramName).get(0).equals(currentParams.getFirst(paramName))) {
                    // if the new value differs from the old value, update the old value
                    currentParams.put(paramName, newParameters.remove(paramName));
                    paramsUpdated = true;
                }
            } else {
                throw new BadRequestQueryException("Cannot update a query parameter without a value: " + paramName, HttpStatus.SC_BAD_REQUEST + "-1");
            }
        }
        return paramsUpdated;
    }
    
    /**
     * Gets a list of queries for the calling user.
     * <p>
     * Returns all matching queries owned by the calling user, filtering by query id and query name.
     *
     * @param queryId
     *            the query id, may be null
     * @param queryName
     *            the query name, may be null
     * @param currentUser
     *            the user who called this method, not null
     * @return a list response containing the matching queries
     * @throws QueryException
     *             if there is an unknown error
     */
    public QueryImplListResponse list(String queryId, String queryName, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: list from {} for queryId: {}, queryName: {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId,
                        queryName);
        
        return list(queryId, queryName, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getDn().subjectDN()));
    }
    
    /**
     * Gets a list of queries for the specified user using admin privileges.
     * <p>
     * Returns all matching queries owned by any user, filtered by user ID, query ID, and query name. <br>
     * Only admin users should be allowed to call this method.
     *
     * @param queryId
     *            the query id, may be null
     * @param queryName
     *            the query name, may be null
     * @param userId
     *            the user whose queries we want to list, may be null
     * @param currentUser
     *            the user who called this method, not null
     * @return a list response containing the matching queries
     * @throws QueryException
     *             if there is an unknown error
     */
    public QueryImplListResponse adminList(String queryId, String queryName, String userId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: adminList from {} for queryId: {}, queryName: {}, userId: {}",
                        ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId, queryName, userId);
        
        return list(queryId, queryName, userId);
    }
    
    /**
     * Gets a list of all matching queries, filtered by user ID, query ID, and query name.
     *
     * @param queryId
     *            the query id, may be null
     * @param queryName
     *            the query name, may be null
     * @param userId
     *            the user whose queries we want to list, may be null
     * @return a list response containing the matching queries
     * @throws QueryException
     *             if there is an unknown error
     */
    private QueryImplListResponse list(String queryId, String queryName, String userId) throws QueryException {
        try {
            List<Query> queries;
            if (queryId != null && !queryId.isEmpty()) {
                // get the query for the given id
                queries = new ArrayList<>();
                
                QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
                if (queryStatus != null) {
                    queries.add(queryStatus.getQuery());
                }
            } else {
                // get all of the queries
                queries = queryStorageCache.getQueryStatus().stream().map(QueryStatus::getQuery).collect(Collectors.toList());
            }
            
            // only keep queries with the given userId and query name
            queries.removeIf(q -> (userId != null && !q.getOwner().equals(userId)) || (queryName != null && !q.getQueryName().equals(queryName)));
            
            QueryImplListResponse response = new QueryImplListResponse();
            response.setQuery(queries);
            return response;
        } catch (Exception e) {
            log.error("Unknown error listing queries for {}", userId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_LISTING_ERROR, e, "Unknown error listing queries for " + userId);
        }
    }
    
    /**
     * Gets the plan for the given query for the calling user.
     *
     * @param queryId
     *            the query id, may be null
     * @param currentUser
     *            the user who called this method, not null
     * @return the query plan for the matching query
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public GenericResponse<String> plan(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: plan from {} for queryId: {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            GenericResponse<String> response = new GenericResponse<>();
            if (queryStatus.getPlan() != null) {
                response.setResult(queryStatus.getPlan());
                response.setHasResults(true);
            }
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error getting plan for query {}", queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_PLAN_ERROR, e, "Unknown error getting plan for query " + queryId);
        }
    }
    
    /**
     * Gets the predictions for the given query for the calling user.
     *
     * @param queryId
     *            the query id, may be null
     * @param currentUser
     *            the user who called this method, not null
     * @return the query predictions for the matching query
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     * @throws QueryException
     *             if query lock acquisition fails
     * @throws QueryException
     *             if query storage fails
     * @throws QueryException
     *             if there is an unknown error
     */
    public GenericResponse<String> predictions(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("Request: predictions from {} for queryId: {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), queryId);
        
        try {
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = validateRequest(queryId, currentUser);
            
            GenericResponse<String> response = new GenericResponse<>();
            if (queryStatus.getPredictions() != null && !queryStatus.getPredictions().isEmpty()) {
                response.setResult(queryStatus.getPredictions().toString());
                response.setHasResults(true);
            }
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error getting predictions for query {}", queryId, e);
            throw new QueryException(DatawaveErrorCode.QUERY_PLAN_ERROR, e, "Unknown error getting predictions for query " + queryId);
        }
    }
    
    private QueryStatus validateRequest(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        return validateRequest(queryId, currentUser, false);
    }
    
    /**
     * Validates the user request by ensuring that the query exists, and the user owns the query or is an admin.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @param adminOverride
     *            whether or not this is an admin request
     * @return a query status object for the specified query
     * @throws NotFoundQueryException
     *             if the query cannot be found
     * @throws UnauthorizedQueryException
     *             if the user doesn't own the query
     */
    private QueryStatus validateRequest(String queryId, ProxiedUserDetails currentUser, boolean adminOverride)
                    throws NotFoundQueryException, UnauthorizedQueryException {
        // does the query exist?
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        if (queryStatus == null) {
            throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, MessageFormat.format("{0}", queryId));
        }
        
        // admins requests can operate on any query, regardless of ownership
        if (!adminOverride) {
            // does the current user own this query?
            String userId = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getDn().subjectDN());
            Query query = queryStatus.getQuery();
            if (!query.getOwner().equals(userId)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", userId, query.getOwner()));
            }
        }
        
        return queryStatus;
    }
    
    /**
     * Receives remote query requests and handles them.
     * <p>
     * Cancels any running next calls when the request method is {@link QueryRequest.Method#CANCEL}. <br>
     * Takes no action for other remote query requests.
     *
     * @param queryRequest
     *            the remote query request, not null
     * @param originService
     *            the address of the service which sent the request
     * @param destinationService
     *            the address of the service which this event was sent to
     */
    @Override
    public void handleRemoteRequest(QueryRequest queryRequest, String originService, String destinationService) {
        try {
            if (queryRequest.getMethod() == QueryRequest.Method.CANCEL) {
                log.trace("Received remote cancel request from {} for {}.", originService, destinationService);
                cancel(queryRequest.getQueryId(), false);
            } else if (queryRequest.getMethod() == QueryRequest.Method.CREATE || queryRequest.getMethod() == QueryRequest.Method.PLAN
                            || queryRequest.getMethod() == QueryRequest.Method.PREDICT) {
                log.trace("Received remote {} request from {} for {}.", queryRequest.getMethod().name(), originService, destinationService);
                if (queryLatchMap.containsKey(queryRequest.getQueryId())) {
                    queryLatchMap.get(queryRequest.getQueryId()).countDown();
                } else {
                    log.warn("Unable to decrement {} latch for query {}", queryRequest.getMethod().name(), queryRequest.getQueryId());
                }
            } else {
                log.debug("No handling specified for remote query request method: {} from {} for {}", queryRequest.getMethod(), originService,
                                destinationService);
            }
        } catch (Exception e) {
            log.error("Unknown error handling remote request: {} from {} for {}", queryRequest, originService, destinationService);
        }
    }
    
    /**
     * Creates and submits an audit record to the audit service.
     * <p>
     * The audit request submission will fail if the audit service is unable to validate the audit message.
     *
     * @param query
     *            the query to be audited, not null
     * @param queryLogic
     *            the query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param currentUser
     *            the user who called this method, not null
     * @throws BadRequestQueryException
     *             if the audit parameters fail validation
     * @throws BadRequestQueryException
     *             if there is an error auditing the query
     */
    protected void audit(Query query, QueryLogic<?> queryLogic, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws BadRequestQueryException {
        Auditor.AuditType auditType = queryLogic.getAuditType(query);
        
        parameters.add(PrivateAuditConstants.AUDIT_TYPE, auditType.name());
        if (auditType != Auditor.AuditType.NONE) {
            // audit the query before execution
            try {
                try {
                    List<String> selectors = queryLogic.getSelectors(query);
                    if (selectors != null && !selectors.isEmpty()) {
                        parameters.put(PrivateAuditConstants.SELECTORS, selectors);
                    }
                } catch (Exception e) {
                    log.error("Error accessing query selector", e);
                }
                
                // is the user didn't set an audit id, use the query id
                if (!parameters.containsKey(AuditParameters.AUDIT_ID)) {
                    parameters.set(AuditParameters.AUDIT_ID, query.getId().toString());
                }
                
                // @formatter:off
                AuditClient.Request auditRequest = new AuditClient.Request.Builder()
                        .withParams(parameters)
                        .withQueryExpression(query.getQuery())
                        .withProxiedUserDetails(currentUser)
                        .withMarking(securityMarking)
                        .withAuditType(auditType)
                        .withQueryLogic(queryLogic.getLogicName())
                        .build();
                // @formatter:on
                
                log.info("[{}] Sending audit request with parameters {}", query.getId(), auditRequest);
                
                auditClient.submit(auditRequest);
            } catch (IllegalArgumentException e) {
                log.error("Error validating audit parameters", e);
                throw new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER, e);
            } catch (Exception e) {
                log.error("Error auditing query", e);
                throw new BadRequestQueryException(DatawaveErrorCode.QUERY_AUDITING_ERROR, e);
            }
        }
    }
    
    /**
     * Gets the pool name for this request.
     * <p>
     * If no pool is specified in the query parameters, the default pool will be used.
     *
     * @return the pool name for this query
     */
    protected String getPoolName() {
        return (queryParameters.getPool() != null) ? queryParameters.getPool() : queryProperties.getDefaultParams().getPool();
    }
    
    /**
     * Gets the pool-specific executor service name.
     *
     * @param poolName
     *            the pool name, null returns *-null
     * @return the pool-specific executor service name
     */
    protected String getPooledExecutorName(String poolName) {
        return String.join("-", Arrays.asList(queryProperties.getExecutorServiceName(), poolName));
    }
    
    /**
     * Publishes a next event for the given query id and pool.
     *
     * @param queryId
     *            the query id, not null
     * @param queryPool
     *            the pool to use, not null
     */
    public void publishNextEvent(String queryId, String queryPool) {
        publishExecutorEvent(QueryRequest.next(queryId), queryPool);
    }
    
    private void publishExecutorEvent(QueryRequest queryRequest, String queryPool) {
        // @formatter:off
        eventPublisher.publishEvent(
                new RemoteQueryRequestEvent(
                        this,
                        busProperties.getId(),
                        getPooledExecutorName(queryPool),
                        queryRequest));
        // @formatter:on
    }
    
    private void publishSelfEvent(QueryRequest queryRequest) {
        // @formatter:off
        eventPublisher.publishEvent(
                new RemoteQueryRequestEvent(
                        this,
                        busProperties.getId(),
                        selfDestination,
                        queryRequest));
        // @formatter:on
    }
    
    private String getSelfDestination() {
        String id = busProperties.getId();
        if (id.contains(":")) {
            return id.substring(0, id.indexOf(":"));
        }
        return id;
    }
    
    /**
     * Gets the maximum number of concurrent query tasks allowed for this logic.
     * <p>
     * If the max concurrent tasks is overridden, that value will be used. <br>
     * Otherwise, the value is determined via the query logic, if defined, or via the configuration default.
     *
     * @param queryLogic
     *            the requested query logic, not null
     * @return the maximum concurrent tasks limit
     */
    protected int getMaxConcurrentTasks(QueryLogic<?> queryLogic) {
        // if there's an override, use it
        if (queryParameters.isMaxConcurrentTasksOverridden()) {
            return queryParameters.getMaxConcurrentTasks();
        }
        // if the query logic has a limit, use it
        else if (queryLogic.getMaxConcurrentTasks() > 0) {
            return queryLogic.getMaxConcurrentTasks();
        }
        // otherwise, use the configuration default
        else {
            return queryProperties.getDefaultParams().getMaxConcurrentTasks();
        }
    }
    
    /**
     * Creates and initializes a query object using the provided query parameters.
     * <p>
     * If a query id is not specified, then a random one will be generated.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param userDn
     *            the user dn, not null
     * @param dnList
     *            the user dn list, not null
     * @param queryId
     *            the desired query id, may be null
     * @return an instantiated query object
     */
    protected Query createQuery(String queryLogicName, MultiValueMap<String,String> parameters, String userDn, List<String> dnList, String queryId) {
        Query q = responseObjectFactory.getQueryImpl();
        q.initialize(userDn, dnList, queryLogicName, queryParameters, queryParameters.getUnknownParameters(parameters));
        q.setColumnVisibility(securityMarking.toColumnVisibilityString());
        q.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
        Thread.currentThread().setUncaughtExceptionHandler(q.getUncaughtExceptionHandler());
        if (queryId != null) {
            q.setId(UUID.fromString(queryId));
        }
        return q;
    }
    
    /**
     * Validates the query parameters, security markings, and instantiates the requested query logic.
     * <p>
     * If the query is not valid, an exception will be thrown.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return the query logic
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     */
    protected QueryLogic<?> validateQuery(String queryLogicName, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws BadRequestQueryException, UnauthorizedQueryException {
        // validate the query parameters
        validateParameters(queryLogicName, parameters);
        
        // create the query logic, and perform query logic parameter validation
        QueryLogic<?> queryLogic = createQueryLogic(queryLogicName, currentUser);
        validateQueryLogic(queryLogic, parameters, currentUser);
        
        // validate the security markings
        validateSecurityMarkings(parameters);
        
        return queryLogic;
    }
    
    /**
     * Performs query parameter validation.
     * <p>
     * If the parameters are not valid, an exception will be thrown.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if an invalid page size was requested
     * @throws BadRequestQueryException
     *             if an invalid page timeout was requested
     * @throws BadRequestQueryException
     *             if an invalid begin and end date was requested
     */
    protected void validateParameters(String queryLogicName, MultiValueMap<String,String> parameters) throws BadRequestQueryException {
        // add query logic name to parameters
        parameters.set(QUERY_LOGIC_NAME, queryLogicName);
        
        log.debug(writeValueAsString(parameters));
        
        // Pull "params" values into individual query parameters for validation on the query logic.
        // This supports the deprecated "params" value (both on the old and new API). Once we remove the deprecated
        // parameter, this code block can go away.
        if (parameters.get(QueryParameters.QUERY_PARAMS) != null) {
            parameters.get(QueryParameters.QUERY_PARAMS).stream().map(QueryUtil::parseParameters).forEach(parameters::addAll);
        }
        
        parameters.remove(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ);
        parameters.remove(AuditParameters.USER_DN);
        parameters.remove(AuditParameters.QUERY_AUDIT_TYPE);
        
        // Ensure that all required parameters exist prior to validating the values.
        try {
            queryParameters.validate(parameters);
        } catch (IllegalArgumentException e) {
            log.error("Unable to validate query parameters", e);
            throw new BadRequestQueryException("Unable to validate query parameters.", e, HttpStatus.SC_BAD_REQUEST + "-1");
        }
        
        // The pageSize and expirationDate checks will always be false when called from the RemoteQueryExecutor.
        // Leaving for now until we can test to ensure that is always the case.
        if (queryParameters.getPagesize() <= 0) {
            log.error("Invalid page size: {}", queryParameters.getPagesize());
            throw new BadRequestQueryException(DatawaveErrorCode.INVALID_PAGE_SIZE);
        }
        
        long pageMinTimeoutMillis = queryProperties.getExpiration().getPageMinTimeoutMillis();
        long pageMaxTimeoutMillis = queryProperties.getExpiration().getPageMaxTimeoutMillis();
        long pageTimeoutMillis = TimeUnit.MINUTES.toMillis(queryParameters.getPageTimeout());
        if (queryParameters.getPageTimeout() != -1 && (pageTimeoutMillis < pageMinTimeoutMillis || pageTimeoutMillis > pageMaxTimeoutMillis)) {
            log.error("Invalid page timeout: {}", queryParameters.getPageTimeout());
            throw new BadRequestQueryException(DatawaveErrorCode.INVALID_PAGE_TIMEOUT);
        }
        
        // Ensure begin date does not occur after the end date (if dates are not null)
        if ((queryParameters.getBeginDate() != null && queryParameters.getEndDate() != null)
                        && queryParameters.getBeginDate().after(queryParameters.getEndDate())) {
            log.error("Invalid begin and/or end date: {}", queryParameters.getBeginDate() + " - " + queryParameters.getEndDate());
            throw new BadRequestQueryException(DatawaveErrorCode.BEGIN_DATE_AFTER_END_DATE);
        }
    }
    
    /**
     * Creates a query logic instance for the given user.
     * <p>
     * The user's roles will be checked when instantiating the query logic.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param currentUser
     *            the user who called this method, not null
     * @return the requested query logic
     * @throws BadRequestQueryException
     *             if the query logic does not exist
     * @throws BadRequestQueryException
     *             if the user does not have the required roles for the query logic
     */
    protected QueryLogic<?> createQueryLogic(String queryLogicName, ProxiedUserDetails currentUser) throws BadRequestQueryException {
        // will throw IllegalArgumentException if not defined
        try {
            return queryLogicFactory.getQueryLogic(queryLogicName, currentUser.getPrimaryUser().getRoles());
        } catch (Exception e) {
            log.error("Failed to get query logic for {}", queryLogicName, e);
            throw new BadRequestQueryException(DatawaveErrorCode.QUERY_LOGIC_ERROR, e);
        }
    }
    
    /**
     * Performs query parameter validation using the query logic.
     * 
     * @param queryLogic
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param currentUser
     *            the user who called this method, not null
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws BadRequestQueryException
     *             if an invalid page size was requested
     * @throws BadRequestQueryException
     *             if an invalid max results override was requested
     * @throws BadRequestQueryException
     *             if an invalid max concurrent tasks override was requested
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     */
    protected void validateQueryLogic(QueryLogic<?> queryLogic, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser)
                    throws BadRequestQueryException, UnauthorizedQueryException {
        try {
            queryLogic.validate(parameters);
        } catch (IllegalArgumentException e) {
            log.error("Unable to validate query parameters with query logic", e);
            throw new BadRequestQueryException("Unable to validate query parameters with query logic.", e, HttpStatus.SC_BAD_REQUEST + "-1");
        }
        
        // always check against the max
        if (queryLogic.getMaxPageSize() > 0 && queryParameters.getPagesize() > queryLogic.getMaxPageSize()) {
            log.error("Invalid page size: {} vs {}", queryParameters.getPagesize(), queryLogic.getMaxPageSize());
            throw new BadRequestQueryException(DatawaveErrorCode.PAGE_SIZE_TOO_LARGE, MessageFormat.format("Max = {0}.", queryLogic.getMaxPageSize()));
        }
        
        // If the user is not privileged, make sure they didn't exceed the limits for the following parameters
        if (!currentUser.getPrimaryUser().getRoles().contains(queryProperties.getPrivilegedRole())) {
            // validate the max results override relative to the max results on a query logic
            // privileged users however can set whatever they want
            if (queryParameters.isMaxResultsOverridden() && queryLogic.getMaxResults() >= 0) {
                if (queryParameters.getMaxResultsOverride() < 0 || (queryLogic.getMaxResults() < queryParameters.getMaxResultsOverride())) {
                    log.error("Invalid max results override: {} vs {}", queryParameters.getMaxResultsOverride(), queryLogic.getMaxResults());
                    throw new BadRequestQueryException(DatawaveErrorCode.INVALID_MAX_RESULTS_OVERRIDE,
                                    MessageFormat.format("Max = {0}.", queryLogic.getMaxResults()));
                }
            }
            
            // validate the max concurrent tasks override relative to the max concurrent tasks on a query logic
            // privileged users however can set whatever they want
            if (queryParameters.isMaxConcurrentTasksOverridden() && queryLogic.getMaxConcurrentTasks() >= 0) {
                if (queryParameters.getMaxConcurrentTasks() < 0 || (queryLogic.getMaxConcurrentTasks() < queryParameters.getMaxConcurrentTasks())) {
                    log.error("Invalid max concurrent tasks override: {} vs {}", queryParameters.getMaxConcurrentTasks(), queryLogic.getMaxConcurrentTasks());
                    throw new BadRequestQueryException(DatawaveErrorCode.INVALID_MAX_CONCURRENT_TASKS_OVERRIDE,
                                    MessageFormat.format("Max = {0}.", queryLogic.getMaxConcurrentTasks()));
                }
            }
        }
        
        // Verify that the calling principal has access to the query logic.
        List<String> dnList = currentUser.getDNs();
        if (!queryLogic.containsDNWithAccess(dnList)) {
            throw new UnauthorizedQueryException("None of the DNs used have access to this query logic: " + dnList, 401);
        }
    }
    
    /**
     * Performs security marking validation using the configured security markings. See {@link SecurityMarking#validate}.
     *
     * @param parameters
     *            the query parameters, not null
     * @throws BadRequestQueryException
     *             if security marking validation fails
     */
    protected void validateSecurityMarkings(MultiValueMap<String,String> parameters) throws BadRequestQueryException {
        try {
            securityMarking.validate(parameters);
        } catch (IllegalArgumentException e) {
            log.error("Failed security markings validation", e);
            throw new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
        }
    }
    
    /**
     * Sets some audit parameters which are used internally to assist with auditing.
     * <p>
     * If any of these parameters exist in the current parameter map, they will first be removed.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param userDn
     *            the user dn, not null
     * @param parameters
     *            the query parameters, not null
     */
    protected void setInternalAuditParameters(String queryLogicName, String userDn, MultiValueMap<String,String> parameters) {
        // Set private audit-related parameters, stripping off any that the user might have passed in first.
        // These are parameters that aren't passed in by the user, but rather are computed from other sources.
        PrivateAuditConstants.stripPrivateParameters(parameters);
        parameters.add(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        parameters.set(PrivateAuditConstants.COLUMN_VISIBILITY, securityMarking.toColumnVisibilityString());
        parameters.add(PrivateAuditConstants.USER_DN, userDn);
    }
    
    private String writeValueAsString(Object object) {
        String stringValue;
        try {
            stringValue = mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            stringValue = String.valueOf(object);
        }
        return stringValue;
    }
}
