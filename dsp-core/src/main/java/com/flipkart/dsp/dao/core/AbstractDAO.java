package com.flipkart.dsp.dao.core;

import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.sg.PredicateType;
import org.hibernate.*;

import javax.persistence.criteria.*;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.time.LocalDateTime;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dropwizard's AbstractDAO
 */
public class AbstractDAO<E> {


    private final Class<?> entityClass;
    private final SessionFactory sessionFactory;

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public AbstractDAO(SessionFactory sessionFactory) {
        this.sessionFactory = checkNotNull(sessionFactory);
        ParameterizedType type = (ParameterizedType) getClass().getGenericSuperclass();
        this.entityClass = (Class<?>) type.getActualTypeArguments()[0];
    }

    /**
     * Returns the current {@link Session}.
     *
     * @return the current session
     */
    protected Session currentSession() {
        return sessionFactory.getCurrentSession();
    }


    /**
     * Convenience method to return a single instance that matches the criteria, or null if the
     * criteria returns no results.
     *
     * @param criteria the {@link Criteria} cosmos to run
     * @return the single result or {@code null}
     * @throws HibernateException if there is more than one matching result
     * @see Criteria#uniqueResult()
     */
    @SuppressWarnings("unchecked")
    protected E uniqueResult(Criteria criteria) throws HibernateException {
        return (E) checkNotNull(criteria).uniqueResult();
    }

    /**
     * Convenience method to return a single instance that matches the cosmos, or null if the cosmos
     * returns no results.
     *
     * @param query the cosmos to run
     * @return the single result or {@code null}
     * @throws HibernateException if there is more than one matching result
     * @see Query#uniqueResult()
     */
    @SuppressWarnings("unchecked")
    protected E uniqueResult(Query query) throws HibernateException {
        return (E) checkNotNull(query).uniqueResult();
    }

    /**
     * Get the results of a {@link Criteria} cosmos.
     *
     * @param criteria the {@link Criteria} cosmos to run
     * @return the list of matched cosmos results
     * @see Criteria#list()
     */
    @SuppressWarnings("unchecked")
    protected List<E> list(Criteria criteria) throws HibernateException {
        return checkNotNull(criteria).list();
    }


    /**
     * Return the persistent instance of {@code <E>} with the given identifier, or {@code null} if
     * there is no such persistent instance. (If the instance, or a proxy for the instance, is
     * already associated with the session, return that instance or proxy.)
     *
     * @param id an identifier
     * @return a persistent instance or {@code null}
     * @throws HibernateException
     * @see Session#get(Class, Serializable)
     */
    @SuppressWarnings("unchecked")
    public E get(Serializable id) {
        return (E) currentSession().get(entityClass, checkNotNull(id));
    }

    /**
     * Either save or update the given instance, depending upon resolution of the unsaved-value
     * checks (see the manual for discussion of unsaved-value checking).
     * This operation cascades to associated instances if the association is mapped with
     * <tt>cascade="save-update"</tt>.
     *
     * @param entity a transient or detached instance containing new or updated state
     * @throws HibernateException
     * @see Session#saveOrUpdate(Object)
     */
    public E persist(E entity) throws HibernateException {
        currentSession().saveOrUpdate(checkNotNull(entity));
        return entity;
    }


    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    protected void persistCollection(Iterable<? extends E> entities) {
        for (E entity : entities) {
            this.persist(entity);
        }
    }

    public void delete(Class<E> clazz, List<SqlParam> sqlParams) {
        CriteriaBuilder criteriaBuilder = getSessionFactory().getCriteriaBuilder();
        CriteriaDelete<E> query = criteriaBuilder.createCriteriaDelete(clazz);
        Root<E> deleteRoot = query.from(clazz);
        List<Predicate> predicates = getQueryPredicates(sqlParams, deleteRoot);
        query.where(predicates.toArray(new Predicate[0]));
        currentSession().createQuery(query).executeUpdate();
    }

    protected Long getCount(Class<E> clazz, List<SqlParam> sqlParams) {
        CriteriaBuilder criteriaBuilder = getSessionFactory().getCriteriaBuilder();
        CriteriaQuery<Long> query = criteriaBuilder.createQuery(Long.class);
        Root<E> countRoot = query.from(clazz);
        Expression<Long> count = criteriaBuilder.count(countRoot);
        query.select(count);
        List<Predicate> predicates = getQueryPredicates(sqlParams, countRoot);
        query.where(predicates.toArray(new Predicate[0]));
        return getSessionFactory().getCurrentSession().createQuery(query).getSingleResult();
    }


    public org.hibernate.query.Query<E> createQuery(Class<E> clazz, List<SqlParam> sqlParams, Map<String/*orderBy*/, String/*order*/> orderByMap) {
        CriteriaBuilder criteriaBuilder = getSessionFactory().getCriteriaBuilder();
        CriteriaQuery<E> query = criteriaBuilder.createQuery(clazz);
        Root<E> root = query.from(clazz);
        query.select(root);
        List<Predicate> predicates = getQueryPredicates(sqlParams, root);
        addOrderBy(query, orderByMap, clazz, root);
        query.where(predicates.toArray(new Predicate[0]));
        return sessionFactory.getCurrentSession().createQuery(query);
    }

    public org.hibernate.query.Query<E> createQuery(Class<E> clazz, List<SqlParam> sqlParams) {
        return createQuery(clazz, sqlParams, new HashMap<>());
    }

    private void addOrderBy(CriteriaQuery<E> criteriaQuery, Map<String/*orderBy*/, String/*order*/> orderByMap, Class<E> clazz, Root<E> root) {
        CriteriaBuilder criteriaBuilder = getSessionFactory().getCriteriaBuilder();
        List<Order> orders = new ArrayList<>();
        orderByMap.forEach((orderBy, order) -> {
            String[] orderBys = orderBy.split("\\.");
            Path expression = root.get(orderBys[0]);
            for (int i = 1; i < orderBys.length; i++) {
                expression = expression.get(orderBys[i]);
            }
            if (order.equalsIgnoreCase("asc"))
                orders.add(criteriaBuilder.asc(expression));
            else
                orders.add(criteriaBuilder.desc(expression));
        });
        criteriaQuery.orderBy(orders);
    }

    private List<Predicate> getQueryPredicates(List<SqlParam> sqlParams, Root<E> root) {
        CriteriaBuilder criteriaBuilder = getSessionFactory().getCriteriaBuilder();
        List<Predicate> predicates = new ArrayList<>();
        for (SqlParam sqlParam : sqlParams) {
            if (Objects.isNull(sqlParam.getParamValues()))
                continue;
            String[] names = sqlParam.getParamName().split("\\.");
            Path expression = root.get(names[0]);
            for (int i = 1; i < names.length; i++) {
                expression = expression.get(names[i]);
            }
            if (sqlParam.getPredicateType().equals(PredicateType.EQUAL))
                predicates.add(criteriaBuilder.equal(expression, sqlParam.getParamValues()));
            else if (sqlParam.getPredicateType().equals(PredicateType.IN))
                addInClause(sqlParam, expression, criteriaBuilder, predicates, PredicateType.IN);
            else if (sqlParam.getPredicateType().equals(PredicateType.GREATER_THAN))
                predicates.add(criteriaBuilder.greaterThan(expression, sqlParam.getParamValues().toString()));
            else if (sqlParam.getPredicateType().equals(PredicateType.LOCAL_DATE_TIME_GREATER_THAN))
                predicates.add(criteriaBuilder.greaterThan(expression.as(LocalDateTime.class), (LocalDateTime) sqlParam.getParamValues()));
            else if (sqlParam.getPredicateType().equals(PredicateType.NOT_IN))
                addInClause(sqlParam, expression, criteriaBuilder, predicates, PredicateType.NOT_IN);
        }
        return predicates;
    }

    private void addInClause(SqlParam sqlParam, Path expression, CriteriaBuilder criteriaBuilder,
                             List<Predicate> predicates, PredicateType predicateType) {
        CriteriaBuilder.In inClause;
        List fieldValues = (List) sqlParam.getParamValues();
        if (fieldValues.size() > 0) {
            inClause = criteriaBuilder.in(expression);
            fieldValues.forEach(inClause::value);
            if (predicateType.equals(PredicateType.NOT_IN)) inClause.not();
            predicates.add(inClause);
        }
    }
}

