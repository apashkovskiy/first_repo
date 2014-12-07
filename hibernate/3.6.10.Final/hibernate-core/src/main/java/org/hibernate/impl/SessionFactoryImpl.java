/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2010, Red Hat Inc. or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat Inc.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.hibernate.impl;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.transaction.TransactionManager;

import org.hibernate.AssertionFailure;
import org.hibernate.Cache;
import org.hibernate.ConnectionReleaseMode;
import org.hibernate.EntityMode;
import org.hibernate.EntityNameResolver;
import org.hibernate.HibernateException;
import org.hibernate.Interceptor;
import org.hibernate.MappingException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.QueryException;
import org.hibernate.SessionFactory;
import org.hibernate.SessionFactoryObserver;
import org.hibernate.StatelessSession;
import org.hibernate.TypeHelper;
import org.hibernate.cache.CacheKey;
import org.hibernate.cache.CollectionRegion;
import org.hibernate.cache.EntityRegion;
import org.hibernate.cache.QueryCache;
import org.hibernate.cache.Region;
import org.hibernate.cache.UpdateTimestampsCache;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cache.access.CollectionRegionAccessStrategy;
import org.hibernate.cache.access.EntityRegionAccessStrategy;
import org.hibernate.cache.impl.CacheDataDescriptionImpl;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.cfg.Settings;
import org.hibernate.connection.ConnectionProvider;
import org.hibernate.context.CurrentSessionContext;
import org.hibernate.context.JTASessionContext;
import org.hibernate.context.ManagedSessionContext;
import org.hibernate.context.ThreadLocalSessionContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.SQLFunctionRegistry;
import org.hibernate.engine.FilterDefinition;
import org.hibernate.engine.Mapping;
import org.hibernate.engine.NamedQueryDefinition;
import org.hibernate.engine.NamedSQLQueryDefinition;
import org.hibernate.engine.ResultSetMappingDefinition;
import org.hibernate.engine.SessionFactoryImplementor;
import org.hibernate.engine.profile.Association;
import org.hibernate.engine.profile.Fetch;
import org.hibernate.engine.profile.FetchProfile;
import org.hibernate.engine.query.QueryPlanCache;
import org.hibernate.engine.query.sql.NativeSQLQuerySpecification;
import org.hibernate.event.EventListeners;
import org.hibernate.exception.SQLExceptionConverter;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.UUIDGenerator;
import org.hibernate.id.factory.IdentifierGeneratorFactory;
import org.hibernate.jdbc.BatcherFactory;
import org.hibernate.mapping.Collection;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.RootClass;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.metadata.CollectionMetadata;
import org.hibernate.persister.PersisterFactory;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.persister.entity.Queryable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.proxy.EntityNotFoundDelegate;
import org.hibernate.stat.ConcurrentStatisticsImpl;
import org.hibernate.stat.Statistics;
import org.hibernate.stat.StatisticsImplementor;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.hbm2ddl.SchemaUpdate;
import org.hibernate.tool.hbm2ddl.SchemaValidator;
import org.hibernate.transaction.TransactionFactory;
import org.hibernate.tuple.Tuplizer;
import org.hibernate.tuple.entity.EntityTuplizer;
import org.hibernate.type.AssociationType;
import org.hibernate.type.Type;
import org.hibernate.type.TypeResolver;
import org.hibernate.util.CollectionHelper;
import org.hibernate.util.EmptyIterator;
import org.hibernate.util.ReflectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Concrete implementation of the <tt>SessionFactory</tt> interface. Has the following
 * responsibilities
 * <ul>
 * <li>caches configuration settings (immutably)
 * <li>caches "compiled" mappings ie. <tt>EntityPersister</tt>s and
 *     <tt>CollectionPersister</tt>s (immutable)
 * <li>caches "compiled" queries (memory sensitive cache)
 * <li>manages <tt>PreparedStatement</tt>s
 * <li> delegates JDBC <tt>Connection</tt> management to the <tt>ConnectionProvider</tt>
 * <li>factory for instances of <tt>SessionImpl</tt>
 * </ul>
 * This class must appear immutable to clients, even if it does all kinds of caching
 * and pooling under the covers. It is crucial that the class is not only thread
 * safe, but also highly concurrent. Synchronization must be used extremely sparingly.
 *
 * @see org.hibernate.connection.ConnectionProvider
 * @see org.hibernate.classic.Session
 * @see org.hibernate.hql.QueryTranslator
 * @see org.hibernate.persister.entity.EntityPersister
 * @see org.hibernate.persister.collection.CollectionPersister
 * @author Gavin King
 */
public final class SessionFactoryImpl implements SessionFactoryImplementor {
	private static final long serialVersionUID = 2228129191920456237L; // pipan was there

	private static final Logger log = LoggerFactory.getLogger(SessionFactoryImpl.class);
	private static final IdentifierGenerator UUID_GENERATOR = UUIDGenerator.buildSessionFactoryUniqueIdentifierGenerator();

	private final String name;
	private final String uuid;

	private final transient Map<String, EntityPersister> entityPersisters;
	private final transient Map<String, ClassMetadata> classMetadata;
	private final transient Map<String, CollectionPersister> collectionPersisters;
	private final transient Map<String, CollectionMetadata> collectionMetadata;
	private final transient Map<String, Set<String>> collectionRolesByEntityParticipant;
	private final transient Map<String, IdentifierGenerator> identifierGenerators;
	private final transient Map<String, NamedQueryDefinition> namedQueries;
	private final transient Map<String, NamedSQLQueryDefinition> namedSqlQueries;
	private final transient Map<String, ResultSetMappingDefinition> sqlResultSetMappings;
	private final transient Map<String, FilterDefinition> filters;
	private final transient Map<String, FetchProfile> fetchProfiles;
	private final transient Map<String, String> imports;
	private final transient Interceptor interceptor;
	private final transient Settings settings;
	private final transient Properties properties;
	private transient SchemaExport schemaExport;
	private final transient TransactionManager transactionManager;
	private final transient QueryCache queryCache;
	private final transient UpdateTimestampsCache updateTimestampsCache;
	private final transient Map<String, QueryCache> queryCaches;
	private final transient ConcurrentMap<String, Region> allCacheRegions = new ConcurrentHashMap<String, Region>();
	private final transient Statistics statistics;
	private final transient EventListeners eventListeners;
	private final transient CurrentSessionContext currentSessionContext;
	private final transient EntityNotFoundDelegate entityNotFoundDelegate;
	private final transient SQLFunctionRegistry sqlFunctionRegistry;
	private final transient SessionFactoryObserver observer;
	private final transient Map<EntityMode, Set<EntityNameResolver>> entityNameResolvers = new HashMap<EntityMode, Set<EntityNameResolver>>();
	private final transient QueryPlanCache queryPlanCache;
	private final transient Cache cacheAccess = new CacheImpl();
	private transient boolean isClosed = false;
	private final transient TypeResolver typeResolver;
	private final transient TypeHelper typeHelper;



	/**
	 * CONSTRUCTOR!!!11
	 * 
	 * @param cfg
	 * @param mapping
	 * @param settings
	 * @param listeners
	 * @param observer
	 */
	public SessionFactoryImpl(
			Configuration cfg,
	        Mapping mapping,
	        Settings settings,
	        EventListeners listeners,
			SessionFactoryObserver observer) throws HibernateException {
		log.info("building session factory");

		this.statistics = new ConcurrentStatisticsImpl( this );
		getStatistics().setStatisticsEnabled( settings.isStatisticsEnabled() );
		log.debug( "Statistics initialized [enabled={}]}", settings.isStatisticsEnabled() );

		this.properties = new Properties();
		this.properties.putAll( cfg.getProperties() );
		this.interceptor = cfg.getInterceptor();
		this.settings = settings;
		this.sqlFunctionRegistry = new SQLFunctionRegistry(settings.getDialect(), cfg.getSqlFunctions());
        this.eventListeners = listeners;
		this.observer = observer != null ? observer : new SessionFactoryObserver() {
			private static final long serialVersionUID = -7573346933307506703L;

			@Override
			public void sessionFactoryCreated(SessionFactory factory) {
				
			}

			@Override
			public void sessionFactoryClosed(SessionFactory factory) {
				
			}
		};

		this.typeResolver = cfg.getTypeResolver().scope( this );
		this.typeHelper = new TypeLocatorImpl( typeResolver );

		this.filters = new HashMap<String, FilterDefinition>();
		this.filters.putAll(cfg.getFilterDefinitions());
		if (log.isDebugEnabled()) {
			log.debug("Session factory constructed with filter configurations : " + filters);
		}

		if (log.isDebugEnabled()) {
			log.debug("instantiating session factory with properties: " + properties);
		}

		// Caches
		settings.getRegionFactory().start( settings, properties );
		this.queryPlanCache = new QueryPlanCache( this );

		//Generators:

		identifierGenerators = new HashMap<String, IdentifierGenerator>();
		Iterator<PersistentClass> classesIterator1 = cfg.getClassMappings();
		while (classesIterator1.hasNext()) {
			PersistentClass model = classesIterator1.next();
			if (!model.isInherited()) {
				IdentifierGenerator generator = model.getIdentifier().createIdentifierGenerator(
						cfg.getIdentifierGeneratorFactory(),
						settings.getDialect(),
				        settings.getDefaultCatalogName(),
				        settings.getDefaultSchemaName(),
				        (RootClass) model
				);
				identifierGenerators.put(model.getEntityName(), generator);
			}
		}


		///////////////////////////////////////////////////////////////////////
		// Prepare persisters and link them up with their cache
		// region/access-strategy

		final String cacheRegionPrefix = settings.getCacheRegionPrefix() == null ? "" : settings.getCacheRegionPrefix() + ".";

		entityPersisters = new HashMap<String, EntityPersister>();
		Map<String, EntityRegionAccessStrategy> entityAccessStrategies = new HashMap<String, EntityRegionAccessStrategy>();
		Map<String,ClassMetadata> classMeta = new HashMap<String,ClassMetadata>();
		Iterator<PersistentClass> classesIterator2 = cfg.getClassMappings();
		while (classesIterator2.hasNext()) {
			final PersistentClass model = classesIterator2.next();
			model.prepareTemporaryTables( mapping, settings.getDialect() );
			final String cacheRegionName = cacheRegionPrefix + model.getRootClass().getCacheRegionName();
			// cache region is defined by the root-class in the hierarchy...
			EntityRegionAccessStrategy accessStrategy = ( EntityRegionAccessStrategy ) entityAccessStrategies.get( cacheRegionName );
			if ( accessStrategy == null && settings.isSecondLevelCacheEnabled() ) {
				final AccessType accessType = AccessType.parse( model.getCacheConcurrencyStrategy() );
				if ( accessType != null ) {
					log.trace( "Building cache for entity data [" + model.getEntityName() + "]" );
					EntityRegion entityRegion = settings.getRegionFactory().buildEntityRegion( cacheRegionName, properties, CacheDataDescriptionImpl.decode( model ) );
					accessStrategy = entityRegion.buildAccessStrategy( accessType );
					entityAccessStrategies.put( cacheRegionName, accessStrategy );
					allCacheRegions.put( cacheRegionName, entityRegion );
				}
			}
			EntityPersister cp = PersisterFactory.createClassPersister( model, accessStrategy, this, mapping );
			entityPersisters.put(model.getEntityName(), cp);
			classMeta.put( model.getEntityName(), cp.getClassMetadata() );
		}
		this.classMetadata = Collections.unmodifiableMap(classMeta);

		Map<String, Set<String>> tmpEntityToCollectionRoleMap = new HashMap<String, Set<String>>();
		collectionPersisters = new HashMap<String, CollectionPersister>();
		collectionMetadata = new HashMap<String, CollectionMetadata>(); // FIXED BY pipan TODO check Hibernate 4.0 for this bug
		
		Iterator<Collection> collections = cfg.getCollectionMappings();
		while (collections.hasNext()) {
			Collection model = collections.next();
			
			final String cacheRegionName = cacheRegionPrefix + model.getCacheRegionName();
			final AccessType accessType = AccessType.parse( model.getCacheConcurrencyStrategy() );
			CollectionRegionAccessStrategy accessStrategy = null;
			if ( accessType != null && settings.isSecondLevelCacheEnabled() ) {
				log.trace( "Building cache for collection data [" + model.getRole() + "]" );
				CollectionRegion collectionRegion = settings.getRegionFactory().buildCollectionRegion( cacheRegionName, properties, CacheDataDescriptionImpl.decode( model ) );
				accessStrategy = collectionRegion.buildAccessStrategy( accessType );
				// entityAccessStrategies.put(cacheRegionName, accessStrategy); // FIXED BY pipan
				allCacheRegions.put( cacheRegionName, collectionRegion );
			}
			CollectionPersister persister = PersisterFactory.createCollectionPersister( cfg, model, accessStrategy, this) ;
			collectionPersisters.put(model.getRole(), persister); // FIXED BY pipan
			collectionMetadata.put(model.getRole(), persister.getCollectionMetadata()); // FIXED BY pipan
			
			Type indexType = persister.getIndexType();
			if (indexType != null && indexType.isAssociationType() && !indexType.isAnyType()) {
				String entityName = ((AssociationType) indexType).getAssociatedEntityName(this);
				Set<String> roles = tmpEntityToCollectionRoleMap.get( entityName );
				if (roles == null) {
					roles = new HashSet<String>();
					tmpEntityToCollectionRoleMap.put(entityName, roles);
				}
				roles.add(persister.getRole());
			}
			
			Type elementType = persister.getElementType();
			if (elementType.isAssociationType() && !elementType.isAnyType()) {
				String entityName = ((AssociationType) elementType).getAssociatedEntityName(this);
				Set<String> roles = tmpEntityToCollectionRoleMap.get(entityName);
				if (roles == null) {
					roles = new HashSet<String>();
					tmpEntityToCollectionRoleMap.put(entityName, roles);
				}
				roles.add(persister.getRole());
			}
		}
		// collectionMetadata = Collections.unmodifiableMap(collectionPersisters); // FIXED BY pipan
		for (Map.Entry<String, Set<String>> entry : tmpEntityToCollectionRoleMap.entrySet()) {
			Set<String> oldValue = entry.getValue();
			Set<String> newValue = Collections.unmodifiableSet(oldValue);
			entry.setValue(newValue);
		}
		collectionRolesByEntityParticipant = Collections.unmodifiableMap(tmpEntityToCollectionRoleMap);

		//Named Queries:
		namedQueries = new HashMap<String, NamedQueryDefinition>(cfg.getNamedQueries());
		namedSqlQueries = new HashMap<String, NamedSQLQueryDefinition>(cfg.getNamedSQLQueries());
		sqlResultSetMappings = new HashMap<String, ResultSetMappingDefinition>(cfg.getSqlResultSetMappings());
		imports = new HashMap<String, String>(cfg.getImports());

		// after *all* persisters and named queries are registered
		for (EntityPersister persister : entityPersisters.values()) {
			persister.postInstantiate();
			registerEntityNameResolvers(persister);
		}
		
		for (CollectionPersister persister : collectionPersisters.values()) {
			persister.postInstantiate();
		}

		//JNDI + Serialization:

		name = settings.getSessionFactoryName();
		try {
			uuid = (String) UUID_GENERATOR.generate(null, null);
		}
		catch (Exception e) {
			throw new AssertionFailure("Could not generate UUID");
		}
		SessionFactoryObjectFactory.addInstance(uuid, name, this, properties);

		log.debug("instantiated session factory");

		if ( settings.isAutoCreateSchema() ) {
			new SchemaExport( cfg, settings ).create( false, true );
		}
		if ( settings.isAutoUpdateSchema() ) {
			new SchemaUpdate( cfg, settings ).execute( false, true );
		}
		if ( settings.isAutoValidateSchema() ) {
			new SchemaValidator( cfg, settings ).validate();
		}
		if ( settings.isAutoDropSchema() ) {
			schemaExport = new SchemaExport( cfg, settings );
		}

		if ( settings.getTransactionManagerLookup()!=null ) {
			log.debug("obtaining JTA TransactionManager");
			transactionManager = settings.getTransactionManagerLookup().getTransactionManager(properties);
		}
		else {
			if ( settings.getTransactionFactory().isTransactionManagerRequired() ) {
				throw new HibernateException("The chosen transaction strategy requires access to the JTA TransactionManager");
			}
			transactionManager = null;
		}

		currentSessionContext = buildCurrentSessionContext();

		if ( settings.isQueryCacheEnabled() ) {
			updateTimestampsCache = new UpdateTimestampsCache(settings, properties);
			queryCache = settings.getQueryCacheFactory()
			        .getQueryCache(null, updateTimestampsCache, settings, properties);
			queryCaches = new HashMap<String,QueryCache>();
			allCacheRegions.put( updateTimestampsCache.getRegion().getName(), updateTimestampsCache.getRegion() );
			allCacheRegions.put( queryCache.getRegion().getName(), queryCache.getRegion() );
		}
		else {
			updateTimestampsCache = null;
			queryCache = null;
			queryCaches = null;
		}

		//checking for named queries
		if ( settings.isNamedQueryStartupCheckingEnabled() ) {
			Map<String, HibernateException> errors = checkNamedQueries();
			if (!errors.isEmpty()) {
				StringBuffer failingQueries = new StringBuffer("Errors in named queries: ");
				Set<String> keys = errors.keySet();
				for (Iterator<String> iterator = keys.iterator(); iterator.hasNext();) {
					String queryName = iterator.next();
					HibernateException e = errors.get(queryName);
					failingQueries.append(queryName);
					if (iterator.hasNext()) {
						failingQueries.append(", ");
					}
					log.error("Error in named query: " + queryName, e);
				}
				throw new HibernateException(failingQueries.toString());
			}
		}

		// EntityNotFoundDelegate
		EntityNotFoundDelegate entityNotFoundDelegate = cfg.getEntityNotFoundDelegate();
		if ( entityNotFoundDelegate == null ) {
			entityNotFoundDelegate = new EntityNotFoundDelegate() {
				public void handleEntityNotFound(String entityName, Serializable id) {
					throw new ObjectNotFoundException( id, entityName );
				}
			};
		}
		this.entityNotFoundDelegate = entityNotFoundDelegate;

		// this needs to happen after persisters are all ready to go...
		this.fetchProfiles = new HashMap<String, FetchProfile>();
		Iterator<org.hibernate.mapping.FetchProfile> itr = cfg.iterateFetchProfiles();
		while (itr.hasNext()) {
			final org.hibernate.mapping.FetchProfile mappingProfile = itr.next();
			
			final FetchProfile fetchProfile = new FetchProfile(mappingProfile.getName());
			for (org.hibernate.mapping.FetchProfile.Fetch mappingFetch : mappingProfile.getFetches()) {
				// resolve the persister owning the fetch
				final String entityName = getImportedClassName( mappingFetch.getEntity() );
				final EntityPersister owner = (entityName == null) ? null : entityPersisters.get(entityName);
				if ( owner == null ) {
					throw new HibernateException(
							"Unable to resolve entity reference [" + mappingFetch.getEntity()
									+ "] in fetch profile [" + fetchProfile.getName() + "]"
					);
				}

				// validate the specified association fetch
				Type associationType = owner.getPropertyType( mappingFetch.getAssociation() );
				if ( associationType == null || !associationType.isAssociationType() ) {
					throw new HibernateException( "Fetch profile [" + fetchProfile.getName() + "] specified an invalid association" );
				}

				// resolve the style
				final Fetch.Style fetchStyle = Fetch.Style.parse( mappingFetch.getStyle() );

				// then construct the fetch instance...
				fetchProfile.addFetch( new Association( owner, mappingFetch.getAssociation() ), fetchStyle );
				( ( Loadable ) owner ).registerAffectingFetchProfile( fetchProfile.getName() );
			}
			fetchProfiles.put(fetchProfile.getName(), fetchProfile);
		}

		this.observer.sessionFactoryCreated( this );
	}



	@Override
	public Properties getProperties() {
		return properties;
	}

	@Override
	public IdentifierGeneratorFactory getIdentifierGeneratorFactory() {
		return null;
	}

	@Override
	public TypeResolver getTypeResolver() {
		return typeResolver;
	}

	private void registerEntityNameResolvers(EntityPersister persister) {
		if (persister.getEntityMetamodel() == null || persister.getEntityMetamodel().getTuplizerMapping() == null) {
			return;
		}
		
		Iterator<Tuplizer> itr = persister.getEntityMetamodel().getTuplizerMapping().iterateTuplizers();
		while (itr.hasNext()) {
			final EntityTuplizer tuplizer = (EntityTuplizer) itr.next(); // can fail, pipan
			registerEntityNameResolvers(tuplizer);
		}
	}

	private void registerEntityNameResolvers(EntityTuplizer tuplizer) {
		EntityNameResolver[] resolvers = tuplizer.getEntityNameResolvers();
		if ( resolvers == null ) {
			return;
		}

		for ( int i = 0; i < resolvers.length; i++ ) {
			registerEntityNameResolver( resolvers[i], tuplizer.getEntityMode() );
		}
	}

	public void registerEntityNameResolver(EntityNameResolver resolver, EntityMode entityMode) {
		Set<EntityNameResolver> resolversForMode = entityNameResolvers.get(entityMode);
		if (resolversForMode == null) {
			resolversForMode = new LinkedHashSet<EntityNameResolver>();
			entityNameResolvers.put(entityMode, resolversForMode);
		}
		resolversForMode.add(resolver);
	}

	public Iterator<EntityNameResolver> iterateEntityNameResolvers(EntityMode entityMode) {
		Set<EntityNameResolver> actualEntityNameResolvers = entityNameResolvers.get(entityMode);
		actualEntityNameResolvers.iterator();
		return actualEntityNameResolvers == null
				? new EmptyIterator<EntityNameResolver>()
				: actualEntityNameResolvers.iterator();
	}

	@Override
	public QueryPlanCache getQueryPlanCache() {
		return queryPlanCache;
	}



	private Map<String, HibernateException> checkNamedQueries() throws HibernateException {
		Map<String, HibernateException> errors = new HashMap<String, HibernateException>();


		// Check named HQL queries
		log.debug("Checking " + namedQueries.size() + " named HQL queries");
		for (Map.Entry<String, NamedQueryDefinition> entry : namedQueries.entrySet()) {
			final String queryName = entry.getKey();
			final NamedQueryDefinition qd = entry.getValue();
			
			log.debug("Checking named query: " + queryName);
			
			// this will throw an error if there's something wrong.
			try {
				//TODO: BUG! this currently fails for named queries for non-POJO entities
				queryPlanCache.getHQLQueryPlan(qd.getQueryString(), false, CollectionHelper.EMPTY_MAP);
			} catch (QueryException e) {
				errors.put(queryName, e);
			} catch (MappingException e) {
				errors.put(queryName, e);
			}
		}


		log.debug("Checking " + namedSqlQueries.size() + " named SQL queries");
		for (Map.Entry<String, NamedSQLQueryDefinition> entry : namedSqlQueries.entrySet()) {
			final String queryName = entry.getKey();
			final NamedSQLQueryDefinition qd = entry.getValue();
			
			log.debug("Checking named SQL query: " + queryName);
			
			// this will throw an error if there's something wrong.
			try {
				// TODO : would be really nice to cache the spec on the query-def so as to not have to re-calc the hash;
				// currently not doable though because of the resultset-ref stuff...
				NativeSQLQuerySpecification spec;
				if (qd.getResultSetRef() != null) {
					ResultSetMappingDefinition definition = sqlResultSetMappings.get(qd.getResultSetRef());
					if (definition == null) {
						throw new MappingException("Unable to find resultset-ref definition: " + qd.getResultSetRef());
					}
					spec = new NativeSQLQuerySpecification(
						qd.getQueryString(), 
						definition.getQueryReturns(), 
						qd.getQuerySpaces()
					);
				} else {
					spec =  new NativeSQLQuerySpecification(
						qd.getQueryString(), 
						qd.getQueryReturns(), 
						qd.getQuerySpaces()
					);
				}
				
				queryPlanCache.getNativeSQLQueryPlan(spec);
			} catch (QueryException e) {
				errors.put(queryName, e);
			} catch (MappingException e) {
				errors.put(queryName, e);
			}
		}

		return errors;
	}



	@Override
	public StatelessSession openStatelessSession() {
		return new StatelessSessionImpl(null, this);
	}

	@Override
	public StatelessSession openStatelessSession(Connection connection) {
		return new StatelessSessionImpl(connection, this);
	}



	@Override
	public org.hibernate.classic.Session openSession(Connection connection) {
		return openSession(connection, interceptor); //prevents this session from adding things to cache
	}

	@Override
	public org.hibernate.classic.Session openSession(Connection connection, Interceptor sessionLocalInterceptor) {
		return openSession(connection, false, Long.MIN_VALUE, sessionLocalInterceptor);
	}

	@Override
	public org.hibernate.classic.Session openSession() throws HibernateException {
		return openSession(interceptor);
	}

	@Override
	public org.hibernate.classic.Session openSession(Interceptor sessionLocalInterceptor) throws HibernateException {
		// note that this timestamp is not correct if the connection provider
		// returns an older JDBC connection that was associated with a
		// transaction that was already begun before openSession() was called
		// (don't know any possible solution to this!)
		long timestamp = settings.getRegionFactory().nextTimestamp();
		return openSession( null, true, timestamp, sessionLocalInterceptor );
	}


	private SessionImpl openSession(
			Connection connection,
		    boolean autoClose,
		    long timestamp,
		    Interceptor sessionLocalInterceptor) {
		return new SessionImpl(
		        connection,
		        this,
		        autoClose,
		        timestamp,
		        sessionLocalInterceptor == null ? interceptor : sessionLocalInterceptor,
		        settings.getDefaultEntityMode(),
		        settings.isFlushBeforeCompletionEnabled(),
		        settings.isAutoCloseSessionEnabled(),
		        settings.getConnectionReleaseMode()
		);
	}



	@Override
	public org.hibernate.classic.Session openTemporarySession() throws HibernateException {
		return new SessionImpl(
				null,
		        this,
		        true,
		        settings.getRegionFactory().nextTimestamp(),
		        interceptor,
		        settings.getDefaultEntityMode(),
		        false,
		        false,
		        ConnectionReleaseMode.AFTER_STATEMENT
			);
	}

	@Override
	public org.hibernate.classic.Session openSession(
			final Connection connection,
	        final boolean flushBeforeCompletionEnabled,
	        final boolean autoCloseSessionEnabled,
	        final ConnectionReleaseMode connectionReleaseMode) throws HibernateException {
		return new SessionImpl(
				connection,
		        this,
		        true,
		        settings.getRegionFactory().nextTimestamp(),
		        interceptor,
		        settings.getDefaultEntityMode(),
		        flushBeforeCompletionEnabled,
		        autoCloseSessionEnabled,
		        connectionReleaseMode
			);
	}


	@Override
	public org.hibernate.classic.Session getCurrentSession() throws HibernateException {
		if (currentSessionContext == null) {
			throw new HibernateException("No CurrentSessionContext configured!");
		}
		return currentSessionContext.currentSession();
	}

	@Override
	public EntityPersister getEntityPersister(String entityName) throws MappingException {
		EntityPersister result = entityPersisters.get(entityName);
		if (result == null) {
			throw new MappingException("Unknown entity: " + entityName);
		}
		return result;
	}

	@Override
	public CollectionPersister getCollectionPersister(String role) throws MappingException {
		CollectionPersister result = collectionPersisters.get(role);
		if (result == null) {
			throw new MappingException("Unknown collection role: " + role);
		}
		return result;
	}



	@Override
	public Settings getSettings() {
		return settings;
	}

	@Override
	public Dialect getDialect() {
		return settings.getDialect();
	}

	@Override
	public Interceptor getInterceptor() {
		return interceptor;
	}

	public TransactionFactory getTransactionFactory() {
		return settings.getTransactionFactory();
	}

	@Override
	public TransactionManager getTransactionManager() {
		return transactionManager;
	}

	@Override
	public SQLExceptionConverter getSQLExceptionConverter() {
		return settings.getSQLExceptionConverter();
	}

	@Override
	public Set<String> getCollectionRolesByEntityParticipant(String entityName) {
		return collectionRolesByEntityParticipant.get( entityName );
	}



	// from javax.naming.Referenceable
	@Override
	public Reference getReference() throws NamingException {
		log.debug("Returning a Reference to the SessionFactory");
		return new Reference(
			SessionFactoryImpl.class.getName(),
		    new StringRefAddr("uuid", uuid),
		    SessionFactoryObjectFactory.class.getName(),
		    null
		);
	}



	private Object readResolve() throws ObjectStreamException {
		log.trace("Resolving serialized SessionFactory");
		// look for the instance by uuid
		Object result = SessionFactoryObjectFactory.getInstance(uuid);
		if (result==null) {
			// in case we were deserialized in a different JVM, look for an instance with the same name
			// (alternatively we could do an actual JNDI lookup here....)
			result = SessionFactoryObjectFactory.getNamedInstance(name);
			if (result==null) {
				throw new InvalidObjectException("Could not find a SessionFactory named: " + name);
			}
			else {
				log.debug("resolved SessionFactory by name");
			}
		}
		else {
			log.debug("resolved SessionFactory by uid");
		}
		return result;
	}



	@Override
	public NamedQueryDefinition getNamedQuery(String queryName) {
		return namedQueries.get(queryName);
	}

	@Override
	public NamedSQLQueryDefinition getNamedSQLQuery(String queryName) {
		return namedSqlQueries.get(queryName);
	}

	@Override
	public ResultSetMappingDefinition getResultSetMapping(String resultSetName) {
		return sqlResultSetMappings.get(resultSetName);
	}

	@Override
	public Type getIdentifierType(String className) throws MappingException {
		return getEntityPersister(className).getIdentifierType();
	}

	@Override
	public String getIdentifierPropertyName(String className) throws MappingException {
		return getEntityPersister(className).getIdentifierPropertyName();
	}



	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		log.trace("deserializing");
		in.defaultReadObject();
		log.debug("deserialized: " + uuid);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		log.debug("serializing: " + uuid);
		out.defaultWriteObject();
		log.trace("serialized");
	}



	@Override
	public Type[] getReturnTypes(String queryString) throws HibernateException {
		return queryPlanCache.getHQLQueryPlan( queryString, false, CollectionHelper.EMPTY_MAP ).getReturnMetadata().getReturnTypes();
	}

	@Override
	public String[] getReturnAliases(String queryString) throws HibernateException {
		return queryPlanCache.getHQLQueryPlan( queryString, false, CollectionHelper.EMPTY_MAP ).getReturnMetadata().getReturnAliases();
	}

	@Override
	public ClassMetadata getClassMetadata(Class<?> persistentClass) throws HibernateException {
		return getClassMetadata(persistentClass.getName());
	}

	@Override
	public CollectionMetadata getCollectionMetadata(String roleName) throws HibernateException {
		return collectionMetadata.get(roleName);
	}

	@Override
	public ClassMetadata getClassMetadata(String entityName) throws HibernateException {
		return classMetadata.get(entityName);
	}



	/**
	 * Return the names of all persistent (mapped) classes that extend or implement the
	 * given class or interface, accounting for implicit/explicit polymorphism settings
	 * and excluding mapped subclasses/joined-subclasses of other classes in the result.
	 */
	@Override
	public String[] getImplementors(String className) throws MappingException {
		final Class<?> clazz;
		try {
			clazz = ReflectHelper.classForName(className);
		} catch (ClassNotFoundException cnfe) {
			return new String[] {className}; //for a dynamic-class
		}

		ArrayList<String> results = new ArrayList<String>();
		for (EntityPersister testPersister : entityPersisters.values()) {
			//test this entity to see if we must query it
			
			if ( testPersister instanceof Queryable ) {
				Queryable testQueryable = (Queryable) testPersister;
				String testClassName = testQueryable.getEntityName();
				boolean isMappedClass = className.equals(testClassName);
				if ( testQueryable.isExplicitPolymorphism() ) {
					if ( isMappedClass ) {
						return new String[] {className}; //NOTE EARLY EXIT
					}
				}
				else {
					if (isMappedClass) {
						results.add(testClassName);
					}
					else {
						final Class<?> mappedClass = testQueryable.getMappedClass( EntityMode.POJO );
						if ( mappedClass!=null && clazz.isAssignableFrom( mappedClass ) ) {
							final boolean assignableSuperclass;
							if ( testQueryable.isInherited() ) {
								Class<?> mappedSuperclass = getEntityPersister( testQueryable.getMappedSuperclass() ).getMappedClass( EntityMode.POJO);
								assignableSuperclass = clazz.isAssignableFrom(mappedSuperclass);
							}
							else {
								assignableSuperclass = false;
							}
							if ( !assignableSuperclass ) {
								results.add( testClassName );
							}
						}
					}
				}
			}
		}
		
		return results.toArray(new String[results.size()]);
	}


	@Override
	public String getImportedClassName(String className) {
		String result = imports.get(className);
		if (result == null) {
			try {
				ReflectHelper.classForName(className);
				return className;
			} catch (ClassNotFoundException cnfe) {
				return null;
			}
		} else {
			return result;
		}
	}



	@Override
	public Map<String, ClassMetadata> getAllClassMetadata() throws HibernateException {
		return classMetadata;
	}

	@Override
	public Map<String, CollectionMetadata> getAllCollectionMetadata() throws HibernateException {
		return collectionMetadata;
	}

	@Override
	public Type getReferencedPropertyType(String className, String propertyName) throws MappingException {
		return getEntityPersister(className).getPropertyType(propertyName);
	}

	@Override
	public ConnectionProvider getConnectionProvider() {
		return settings.getConnectionProvider();
	}



	/**
	 * Closes the session factory, releasing all held resources.
	 *
	 * <ol>
	 * <li>cleans up used cache regions and "stops" the cache provider.
	 * <li>close the JDBC connection
	 * <li>remove the JNDI binding
	 * </ol>
	 *
	 * Note: Be aware that the sessionfactory instance still can
	 * be a "heavy" object memory wise after close() has been called.  Thus
	 * it is important to not keep referencing the instance to let the garbage
	 * collector release the memory.
	 */
	@Override
	public void close() throws HibernateException {
		if (isClosed) {
			log.trace("already closed");
			return;
		}

		log.info("closing");

		isClosed = true;

		for (EntityPersister p : entityPersisters.values()) {
			if (p.hasCache()) {
				p.getCacheAccessStrategy().getRegion().destroy();
			}
		}

		for (CollectionPersister p : collectionPersisters.values()) {
			if (p.hasCache()) {
				p.getCacheAccessStrategy().getRegion().destroy();
			}
		}

		if (settings.isQueryCacheEnabled())  {
			queryCache.destroy();
			for (QueryCache cache : queryCaches.values()) {
				cache.destroy();
			}
			updateTimestampsCache.destroy();
		}

		settings.getRegionFactory().stop();

		if (settings.isAutoDropSchema()) {
			schemaExport.drop(false, true);
		}

		try {
			settings.getConnectionProvider().close();
		} finally {
			SessionFactoryObjectFactory.removeInstance(uuid, name, properties);
		}

		observer.sessionFactoryClosed(this);
		eventListeners.destroyListeners();
	}



	private class CacheImpl implements Cache {
		@Override
		public boolean containsEntity(Class<?> entityClass, Serializable identifier) {
			return containsEntity(entityClass.getName(), identifier);
		}

		@Override
		public boolean containsEntity(String entityName, Serializable identifier) {
			EntityPersister p = getEntityPersister(entityName);
			CacheKey cacheKey = buildCacheKey(identifier, p);
			return p.hasCache() && 
				p.getCacheAccessStrategy().getRegion().contains(cacheKey);
		}

		@Override
		public void evictEntity(Class<?> entityClass, Serializable identifier) {
			evictEntity(entityClass.getName(), identifier);
		}

		@Override
		public void evictEntity(String entityName, Serializable identifier) {
			EntityPersister p = getEntityPersister(entityName);
			if (p.hasCache()) {
				if (log.isDebugEnabled()) {
					log.debug(
						"evicting second-level cache: " + 
						MessageHelper.infoString(p, identifier, SessionFactoryImpl.this)
					);
				}
				CacheKey cacheKey = buildCacheKey(identifier, p);
				p.getCacheAccessStrategy().evict(cacheKey);
			}
		}

		private CacheKey buildCacheKey(Serializable identifier, EntityPersister p) {
			return new CacheKey(
				identifier, 
				p.getIdentifierType(), 
				p.getRootEntityName(), 
				EntityMode.POJO, 
				SessionFactoryImpl.this
			);
		}

		@Override
		public void evictEntityRegion(Class<?> entityClass) {
			evictEntityRegion(entityClass.getName());
		}

		@Override
		public void evictEntityRegion(String entityName) {
			EntityPersister p = getEntityPersister(entityName);
			if (p.hasCache()) {
				if (log.isDebugEnabled()) {
					log.debug("evicting second-level cache: " + p.getEntityName());
				}
				p.getCacheAccessStrategy().evictAll();
			}
		}

		@Override
		public void evictEntityRegions() {
			for (String entityName : entityPersisters.keySet()) {
				evictEntityRegion(entityName);
			}
		}

		@Override
		public boolean containsCollection(String role, Serializable ownerIdentifier) {
			CollectionPersister p = getCollectionPersister(role);
			CacheKey cacheKey = buildCacheKey(ownerIdentifier, p);
			return p.hasCache() && 
				p.getCacheAccessStrategy().getRegion().contains(cacheKey);
		}

		@Override
		public void evictCollection(String role, Serializable ownerIdentifier) {
			CollectionPersister p = getCollectionPersister(role);
			if (p.hasCache()) {
				if (log.isDebugEnabled()) {
					log.debug(
						"evicting second-level cache: " + 
						MessageHelper.collectionInfoString(p, ownerIdentifier, SessionFactoryImpl.this)
					);
				}
				CacheKey cacheKey = buildCacheKey(ownerIdentifier, p);
				p.getCacheAccessStrategy().evict(cacheKey);
			}
		}

		private CacheKey buildCacheKey(Serializable ownerIdentifier, CollectionPersister p) {
			return new CacheKey(
				ownerIdentifier, 
				p.getKeyType(), 
				p.getRole(), 
				EntityMode.POJO, 
				SessionFactoryImpl.this
			);
		}

		@Override
		public void evictCollectionRegion(String role) {
			CollectionPersister p = getCollectionPersister(role);
			if (p.hasCache()) {
				if (log.isDebugEnabled()) {
					log.debug("evicting second-level cache: " + p.getRole());
				}
				p.getCacheAccessStrategy().evictAll();
			}
		}

		@Override
		public void evictCollectionRegions() {
			for (String collectionRole : collectionPersisters.keySet()) {
				evictCollectionRegion(collectionRole);
			}
		}

		@Override
		public boolean containsQuery(String regionName) {
			return queryCaches.get(regionName) != null;
		}

		@Override
		public void evictDefaultQueryRegion() {
			if (settings.isQueryCacheEnabled()) {
				queryCache.clear();
			}
		}

		@Override
		public void evictQueryRegion(String regionName) {
			if (regionName == null) {
				throw new NullPointerException(
					"Region-name cannot be null (use Cache#evictDefaultQueryRegion to evict the default query cache)"
				);
			} else {
				if (settings.isQueryCacheEnabled()) {
					QueryCache namedQueryCache = queryCaches.get(regionName);
					if (namedQueryCache != null) {
						namedQueryCache.clear();
						// TODO : cleanup entries in queryCaches + allCacheRegions ?
					}
				}
			}
		}

		@Override
		public void evictQueryRegions() {
			for (QueryCache queryCache : queryCaches.values()) {
				queryCache.clear();
				// TODO : cleanup entries in queryCaches + allCacheRegions ?
			}
		}
	}



	@Override
	public Cache getCache() {
		return cacheAccess;
	}

	@Override
	public void evictEntity(String entityName, Serializable id) throws HibernateException {
		getCache().evictEntity(entityName, id);
	}

	@Override
	public void evictEntity(String entityName) throws HibernateException {
		getCache().evictEntityRegion(entityName);
	}

	@Override
	public void evict(Class<?> persistentClass, Serializable id) throws HibernateException {
		getCache().evictEntity(persistentClass, id);
	}

	@Override
	public void evict(Class<?> persistentClass) throws HibernateException {
		getCache().evictEntityRegion(persistentClass);
	}

	@Override
	public void evictCollection(String roleName, Serializable id) throws HibernateException {
		getCache().evictCollection(roleName, id);
	}

	@Override
	public void evictCollection(String roleName) throws HibernateException {
		getCache().evictCollectionRegion(roleName);
	}

	@Override
	public void evictQueries() throws HibernateException {
		if (settings.isQueryCacheEnabled()) {
			queryCache.clear();
		}
	}

	@Override
	public void evictQueries(String regionName) throws HibernateException {
		getCache().evictQueryRegion(regionName);
	}

	@Override
	public UpdateTimestampsCache getUpdateTimestampsCache() {
		return updateTimestampsCache;
	}

	@Override
	public QueryCache getQueryCache() {
		return queryCache;
	}


	@Override
	public QueryCache getQueryCache(String regionName) throws HibernateException {
		if ( regionName == null ) {
			return getQueryCache();
		}

		if ( !settings.isQueryCacheEnabled() ) {
			return null;
		}

		QueryCache currentQueryCache = queryCaches.get( regionName );
		if ( currentQueryCache == null ) {
			currentQueryCache = settings.getQueryCacheFactory().getQueryCache( regionName, updateTimestampsCache, settings, properties );
			queryCaches.put( regionName, currentQueryCache );
			allCacheRegions.put( currentQueryCache.getRegion().getName(), currentQueryCache.getRegion() );
		}

		return currentQueryCache;
	}


	@Override
	public Region getSecondLevelCacheRegion(String regionName) {
		return allCacheRegions.get(regionName);
	}

	@Override
	public Map<String, Region> getAllSecondLevelCacheRegions() {
		return new HashMap<String, Region>(allCacheRegions);
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public Statistics getStatistics() {
		return statistics;
	}

	public StatisticsImplementor getStatisticsImplementor() {
		return (StatisticsImplementor) statistics;
	}

	@Override
	public FilterDefinition getFilterDefinition(String filterName) throws HibernateException {
		FilterDefinition def = filters.get(filterName);
		if (def == null) {
			throw new HibernateException("No such filter configured [" + filterName + "]");
		}
		return def;
	}

	@Override
	public boolean containsFetchProfileDefinition(String name) {
		return fetchProfiles.containsKey(name);
	}

	@Override
	public Set<String> getDefinedFilterNames() {
		return filters.keySet();
	}

	public BatcherFactory getBatcherFactory() {
		return settings.getBatcherFactory();
	}

	@Override
	public IdentifierGenerator getIdentifierGenerator(String rootEntityName) {
		return identifierGenerators.get(rootEntityName);
	}



	private CurrentSessionContext buildCurrentSessionContext() {
		String impl = properties.getProperty( Environment.CURRENT_SESSION_CONTEXT_CLASS );
		// for backward-compatability
		if ( impl == null && transactionManager != null ) {
			impl = "jta";
		}

		if ( impl == null ) {
			return null;
		}
		else if ( "jta".equals( impl ) ) {
			if ( settings.getTransactionFactory().areCallbacksLocalToHibernateTransactions() ) {
				log.warn( "JTASessionContext being used with JDBCTransactionFactory; auto-flush will not operate correctly with getCurrentSession()" );
			}
			return new JTASessionContext( this );
		}
		else if ( "thread".equals( impl ) ) {
			return new ThreadLocalSessionContext( this );
		}
		else if ( "managed".equals( impl ) ) {
			return new ManagedSessionContext( this );
		}
		else {
			try {
				Class<?> implClass = ReflectHelper.classForName(impl);
				Constructor<?> constructor = implClass.getConstructor(new Class[] {SessionFactoryImplementor.class});
				return (CurrentSessionContext) constructor.newInstance(new Object[] {this});
			} catch( Throwable t ) {
				log.error( "Unable to construct current session context [" + impl + "]", t );
				return null;
			}
		}
	}



	public EventListeners getEventListeners() {
		return eventListeners;
	}

	@Override
	public EntityNotFoundDelegate getEntityNotFoundDelegate() {
		return entityNotFoundDelegate;
	}

	@Override
	public SQLFunctionRegistry getSqlFunctionRegistry() {
		return sqlFunctionRegistry;
	}

	@Override
	public FetchProfile getFetchProfile(String name) {
		return fetchProfiles.get(name);
	}

	@Override
	public SessionFactoryObserver getFactoryObserver() {
		return observer;
	}

	@Override
	public TypeHelper getTypeHelper() {
		return typeHelper;
	}



	/**
	 * Custom serialization hook used during Session serialization.
	 *
	 * @param oos The stream to which to write the factory
	 * @throws IOException Indicates problems writing out the serial data stream
	 */
	void serialize(ObjectOutputStream oos) throws IOException {
		oos.writeUTF( uuid );
		oos.writeBoolean( name != null );
		if ( name != null ) {
			oos.writeUTF( name );
		}
	}


	/**
	 * Custom deserialization hook used during Session deserialization.
	 *
	 * @param ois The stream from which to "read" the factory
	 * @return The deserialized factory
	 * @throws IOException indicates problems reading back serial data stream
	 * @throws ClassNotFoundException indicates problems reading back serial data stream
	 */
	static SessionFactoryImpl deserialize(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		String uuid = ois.readUTF();
		boolean isNamed = ois.readBoolean();
		String name = null;
		if ( isNamed ) {
			name = ois.readUTF();
		}
		Object result = SessionFactoryObjectFactory.getInstance( uuid );
		if ( result == null ) {
			log.trace( "could not locate session factory by uuid [" + uuid + "] during session deserialization; trying name" );
			if ( isNamed ) {
				result = SessionFactoryObjectFactory.getNamedInstance( name );
			}
			if ( result == null ) {
				throw new InvalidObjectException( "could not resolve session factory during session deserialization [uuid=" + uuid + ", name=" + name + "]" );
			}
		}
		return ( SessionFactoryImpl ) result;
	}
}
