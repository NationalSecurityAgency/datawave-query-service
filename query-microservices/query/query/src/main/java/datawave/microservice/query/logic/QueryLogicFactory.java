package datawave.microservice.query.logic;

import datawave.webservice.query.exception.QueryException;

import java.util.Collection;
import java.util.List;

public interface QueryLogicFactory {
    
    /**
     *
     * @param name
     *            name of query logic
     * @return new instance of QueryLogic class
     * @throws IllegalArgumentException
     *             if query logic name does not exist
     */
    QueryLogic<?> getQueryLogic(String name, Collection<String> userRoles) throws QueryException, IllegalArgumentException, CloneNotSupportedException;
    
    QueryLogic<?> getQueryLogic(String name) throws QueryException, IllegalArgumentException, CloneNotSupportedException;
    
    List<QueryLogic<?>> getQueryLogicList();
}
