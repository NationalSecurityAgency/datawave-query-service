package datawave.microservice.query.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Iterators;
import datawave.microservice.query.logic.BaseQueryLogic;
import datawave.util.TableName;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import javax.xml.bind.annotation.XmlTransient;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

/**
 * <p>
 * A basic query configuration object that contains the information needed to run a query.
 * </p>
 *
 * <p>
 * Provides some "expected" default values for parameters. This configuration object also encapsulates iterators and their options that would be set on a
 * {@link BatchScanner}.
 * </p>
 *
 */
public abstract class GenericQueryConfiguration {
    // is this execution expected to be checkpointable (changes how we allocate ranges to scanners)
    private boolean checkpointable = false;
    
    private Connector connector = null;
    private Set<Authorizations> authorizations = Collections.singleton(Authorizations.EMPTY);
    
    private Query query = null;
    
    // Leave in a top-level query for backwards-compatibility purposes
    private String queryString = null;
    
    private Date beginDate = null;
    private Date endDate = null;
    
    // The max number of next + seek calls made by the underlying iterators
    private Long maxWork = -1L;
    
    protected int baseIteratorPriority = 100;
    
    // Table name
    private String tableName = TableName.SHARD;
    
    private Iterator<QueryData> queries = Collections.emptyIterator();
    
    protected boolean bypassAccumulo;
    
    /**
     * Empty default constructor
     */
    public GenericQueryConfiguration() {
        
    }
    
    /**
     * Pulls the table name, max query results, and max rows to scan from the provided argument
     *
     * @param configuredLogic
     *            A pre-configured BaseQueryLogic to initialize the Configuration with
     */
    public GenericQueryConfiguration(BaseQueryLogic<?> configuredLogic) {
        this(configuredLogic.getConfig());
    }
    
    public GenericQueryConfiguration(GenericQueryConfiguration genericConfig) {
        this.setBaseIteratorPriority(genericConfig.getBaseIteratorPriority());
        this.setBypassAccumulo(genericConfig.getBypassAccumulo());
        this.setAuthorizations(genericConfig.getAuthorizations());
        this.setBeginDate(genericConfig.getBeginDate());
        this.setConnector(genericConfig.getConnector());
        this.setEndDate(genericConfig.getEndDate());
        this.setMaxWork(genericConfig.getMaxWork());
        this.setQueries(genericConfig.getQueries());
        this.setQueryString(genericConfig.getQueryString());
        this.setTableName(genericConfig.getTableName());
    }
    
    /**
     * Return the configured {@code Iterator<QueryData>}
     *
     * @return
     */
    public Iterator<QueryData> getQueries() {
        return Iterators.unmodifiableIterator(this.queries);
    }
    
    /**
     * Set the queries to be run.
     *
     * @param queries
     */
    public void setQueries(Iterator<QueryData> queries) {
        this.queries = queries;
    }
    
    public boolean isCheckpointable() {
        return checkpointable;
    }
    
    public void setCheckpointable(boolean checkpointable) {
        this.checkpointable = checkpointable;
    }
    
    @JsonIgnore
    @XmlTransient
    public Connector getConnector() {
        return connector;
    }
    
    public void setConnector(Connector connector) {
        this.connector = connector;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public void setQuery(Query query) {
        this.query = query;
    }
    
    public void setQueryString(String query) {
        this.queryString = query;
    }
    
    public String getQueryString() {
        return queryString;
    }
    
    public Set<Authorizations> getAuthorizations() {
        return authorizations;
    }
    
    public void setAuthorizations(Set<Authorizations> auths) {
        this.authorizations = auths;
    }
    
    public int getBaseIteratorPriority() {
        return baseIteratorPriority;
    }
    
    public void setBaseIteratorPriority(final int baseIteratorPriority) {
        this.baseIteratorPriority = baseIteratorPriority;
    }
    
    public Date getBeginDate() {
        return beginDate;
    }
    
    public void setBeginDate(Date beginDate) {
        this.beginDate = beginDate;
    }
    
    public Date getEndDate() {
        return endDate;
    }
    
    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }
    
    public Long getMaxWork() {
        return maxWork;
    }
    
    public void setMaxWork(Long maxWork) {
        this.maxWork = maxWork;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public boolean getBypassAccumulo() {
        return bypassAccumulo;
    }
    
    public void setBypassAccumulo(boolean bypassAccumulo) {
        this.bypassAccumulo = bypassAccumulo;
    }
    
    /**
     * Checks for non-null, sane values for the configured values
     *
     * @return True if all of the encapsulated values have legitimate values, otherwise false
     */
    public boolean canRunQuery() {
        // Ensure we were given connector and authorizations
        if (null == this.getConnector() || null == this.getAuthorizations()) {
            return false;
        }
        
        // Ensure valid dates
        if (null == this.getBeginDate() || null == this.getEndDate() || endDate.before(beginDate)) {
            return false;
        }
        
        // A non-empty table was given
        if (null == getTableName() || this.getTableName().isEmpty()) {
            return false;
        }
        
        // At least one QueryData was provided
        if (null == this.queries) {
            return false;
        }
        
        return true;
    }
}
