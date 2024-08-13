package datawave.microservice.query;

import com.codahale.metrics.annotation.Timed;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.result.QueryStatusDetailsResponse;
import datawave.microservice.query.result.QueryStatusResponse;
import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.query.exception.QueryException;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@RestController
@RequestMapping(path = "/v1/queryStatus", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryStatusController {
    private final QueryManagementService queryManagementService;

    public QueryStatusController(QueryManagementService queryManagementService) {
        this.queryManagementService = queryManagementService;
    }

    /**
     * @see QueryManagementService#adminList(String, String, String, DatawaveUserDetails)
     */
    @Timed(name = "dw.queryStatus.list", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "list", method = {RequestMethod.GET}, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<QueryStatusResponse> list(@Parameter(description = "The user DN") @RequestParam(required = false) String userDn,
                                          @Parameter(description = "The query logic") @RequestParam(required = false) String queryLogic,
                                          @Parameter(description = "The query pool") @RequestParam(required = false) String queryPool,
                                          @Parameter(description = "The query state") @RequestParam(required = false) QueryStatus.QUERY_STATE queryState,
                                          @Parameter(description = "The create stage") @RequestParam(required = false) QueryStatus.CREATE_STAGE createStage) {
        List<Predicate<QueryStatus>> predicates = new ArrayList<>(5);
        if (null != userDn) {
            predicates.add((queryStatus -> Arrays.asList(queryStatus.getCurrentUser().getDNs()).contains(userDn)));
        }
        if (null != queryLogic) {
            predicates.add(queryStatus -> queryStatus.getQueryKey().getQueryLogic().equals(queryLogic));
        }
        if (null != queryPool) {
            predicates.add(queryStatus -> queryStatus.getQueryKey().getQueryPool().equals(queryPool));
        }
        if (null != queryState) {
            predicates.add(queryStatus -> queryStatus.getQueryState() == queryState);
        }
        if (null != createStage) {
            predicates.add(queryStatus -> queryStatus.getCreateStage() == createStage);
        }

        return queryManagementService.adminListQueryStatus().stream()
                .filter(queryStatus -> predicates.stream().allMatch(predicate -> predicate.test(queryStatus))).map(QueryStatusResponse::new)
                .collect(Collectors.toList());
    }

    /**
     * @see QueryManagementService#adminList(String, String, String, DatawaveUserDetails)
     */
    @Timed(name = "dw.queryStatus.get", absolute = true)
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/get", method = {RequestMethod.GET}, produces = MediaType.APPLICATION_JSON_VALUE)
    public QueryStatusDetailsResponse get(@Parameter(description = "The query ID") @PathVariable String queryId) throws QueryException {
        Optional<QueryStatus> queryStatus = queryManagementService.adminListQueryStatus().stream()
                .filter((q) -> q.getQuery().getId().toString().equals(queryId)).findFirst();
        if (queryStatus.isPresent()) {
            return new QueryStatusDetailsResponse(queryStatus.get());
        } else {
            throw new QueryException("Unknown problem");
        }
    }
}
