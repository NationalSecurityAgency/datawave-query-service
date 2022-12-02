package datawave.microservice.query.mapreduce;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.mapreduce.config.MapReduceQueryProperties;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.mr.MapReduceInfoResponseList;
import datawave.webservice.results.mr.MapReduceJobDescriptionList;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.util.Map;

@Tag(name = "MapReduce Query Controller /v1", description = "DataWave MapReduce Query Management",
                externalDocs = @ExternalDocumentation(description = "MapReduce Query Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-mapreduce-query-service"))
@RestController
@RequestMapping(path = "/v1/mapreduce", produces = MediaType.APPLICATION_JSON_VALUE)
@ConditionalOnProperty(name = MapReduceQueryProperties.PREFIX + ".enabled", havingValue = "true", matchIfMissing = true)
public class MapReduceQueryController {
    private final MapReduceQueryManagementService mapReduceQueryManagementService;
    
    public MapReduceQueryController(MapReduceQueryManagementService mapReduceQueryManagementService) {
        this.mapReduceQueryManagementService = mapReduceQueryManagementService;
    }
    
    /**
     * @see MapReduceQueryManagementService#listConfigurations(String, ProxiedUserDetails)
     */
    @RequestMapping(path = "listConfigurations", method = RequestMethod.GET, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public MapReduceJobDescriptionList listConfigurations(
                    @Parameter(description = "The job type") @RequestParam(required = false, defaultValue = "none") String jobType,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) {
        return mapReduceQueryManagementService.listConfigurations(jobType, currentUser);
    }
    
    /**
     * @see MapReduceQueryManagementService#oozieSubmit(MultiValueMap, ProxiedUserDetails)
     */
    @RequestMapping(path = "oozieSubmit", method = RequestMethod.POST, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> oozieSubmit(@Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.oozieSubmit(parameters, currentUser);
    }
    
    /**
     * @see MapReduceQueryManagementService#submit(MultiValueMap, ProxiedUserDetails)
     */
    @RequestMapping(path = "submit", method = RequestMethod.POST, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> submit(@Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.submit(parameters, currentUser);
    }
    
    /**
     * @see MapReduceQueryManagementService#cancel(String, ProxiedUserDetails)
     */
    @RequestMapping(path = "{id}/cancel", method = {RequestMethod.POST, RequestMethod.PUT}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<Boolean> cancel(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.cancel(id, currentUser);
    }
    
    /**
     * @see MapReduceQueryManagementService#adminCancel(String, ProxiedUserDetails)
     */
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{id}/adminCancel", method = {RequestMethod.POST, RequestMethod.PUT}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<Boolean> adminCancel(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.adminCancel(id, currentUser);
    }
    
    /**
     * @see MapReduceQueryManagementService#restart(String, ProxiedUserDetails)
     */
    @RequestMapping(path = "{id}/restart", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> restart(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.restart(id, currentUser);
    }
    
    /**
     * @see MapReduceQueryManagementService#list(String, ProxiedUserDetails)
     */
    @RequestMapping(path = "{id}/list", method = RequestMethod.GET, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public MapReduceInfoResponseList list(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.list(id, currentUser);
    }
    
    /**
     * @see MapReduceQueryManagementService#getFile(String,String,ProxiedUserDetails)
     */
    @RequestMapping(path = "{id}/getFile/{fileName}", method = RequestMethod.GET, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> getFile(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @Parameter(description = "The file name") @PathVariable String fileName, @AuthenticationPrincipal ProxiedUserDetails currentUser)
                    throws QueryException {
        final Map.Entry<FileStatus,FSDataInputStream> resultFile = mapReduceQueryManagementService.getFile(id, fileName, currentUser);
        
        // @formatter:off
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"").body(outputStream -> {
                    try (FSDataInputStream inputStream = resultFile.getValue()) {
                        IOUtils.copy(inputStream, outputStream);
                    }
                });
        // @formatter:on
    }
    
    // TODO: This endpoint triggers the following warning, which we should resolve
    // !!!
    // An Executor is required to handle java.util.concurrent.Callable return values.
    // Please, configure a TaskExecutor in the MVC config under "async support".
    // The SimpleAsyncTaskExecutor currently in use is not suitable under load.
    // ------------------------------
    // Request URI: '/query/v1/mapreduce/<mapreduce query id>/getAllFiles'
    // !!!
    /**
     * @see MapReduceQueryManagementService#getAllFiles(String,ProxiedUserDetails)
     */
    @RequestMapping(path = "{id}/getAllFiles", method = RequestMethod.GET, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> getAllFiles(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        final Map<FileStatus,FSDataInputStream> resultFiles = mapReduceQueryManagementService.getAllFiles(id, currentUser);
        
        // @formatter:off
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + id + ".tar\"")
                .body(outputStream -> {
                    TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(outputStream);
                    tarArchiveOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                    try {
                        for (Map.Entry<FileStatus, FSDataInputStream> resultFile : resultFiles.entrySet()) {
                            String fileName = resultFile.getKey().getPath().toString();
                            TarArchiveEntry entry = new TarArchiveEntry(id + "/" + fileName, false);
                            entry.setSize(resultFile.getKey().getLen());
                            tarArchiveOutputStream.putArchiveEntry(entry);
                            try {
                                IOUtils.copy(resultFile.getValue(), tarArchiveOutputStream);
                            } finally {
                                resultFile.getValue().close();
                            }
                            tarArchiveOutputStream.closeArchiveEntry();
                        }
                        tarArchiveOutputStream.finish();
                    } finally {
                        try {
                            tarArchiveOutputStream.close();
                        } catch (IOException ioe) {
                            // do nothing
                        }
                    }
                });
        // @formatter:on
    }
    
    /**
     * @see MapReduceQueryManagementService#list(ProxiedUserDetails)
     */
    @RequestMapping(path = "list", method = RequestMethod.GET, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public MapReduceInfoResponseList list(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.list(currentUser);
    }
    
    /**
     * @see MapReduceQueryManagementService#remove(String, ProxiedUserDetails)
     */
    @RequestMapping(path = "{id}/remove", method = RequestMethod.DELETE, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse remove(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.remove(id, currentUser);
    }
    
    /**
     * @see MapReduceQueryManagementService#adminRemove(String, ProxiedUserDetails)
     */
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{id}/adminRemove", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminRemove(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.adminRemove(id, currentUser);
    }
    
    @RequestMapping(path = "updateState", method = RequestMethod.GET, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse updateState(@RequestParam String jobId, @RequestParam String jobStatus) throws QueryException {
        return mapReduceQueryManagementService.updateState(jobId, jobStatus);
    }
}
