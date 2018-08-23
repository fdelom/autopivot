/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.av.autopivot.config.pivot;

import static com.quartetfs.fwk.types.impl.ExtendedPluginInjector.inject;

import java.util.Arrays;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import com.activeviam.health.monitor.impl.HealthCheckAgent;
import com.av.autopivot.AutoPivotDiscoveryCreator;
import com.av.autopivot.AutoPivotGenerator;
import com.av.autopivot.config.contentserver.ContentServiceConfig;
import com.av.autopivot.config.datastore.DatastoreDescriptionConfig;
import com.av.autopivot.config.datastore.DatastoreServiceConfig;
import com.av.autopivot.config.properties.AutoPivotProperties;
import com.av.autopivot.config.source.SourceConfig;
import com.av.autopivot.config.ui.ActiveUIResourceServerConfig;
import com.av.autopivot.security.ActivePivotBranchPermissionsManagerConfig;
import com.av.autopivot.security.AutoPivotCorsFilterConfig;
import com.av.autopivot.security.SecurityConfig;
import com.qfs.content.cfg.impl.ContentServerWebSocketServicesConfig;
import com.qfs.distribution.security.IDistributedSecurityManager;
import com.qfs.messenger.IDistributedMessenger;
import com.qfs.pivot.content.impl.DynamicActivePivotContentServiceMBean;
import com.qfs.server.cfg.IActivePivotConfig;
import com.qfs.server.cfg.IDatastoreConfig;
import com.qfs.server.cfg.IJwtConfig;
import com.qfs.server.cfg.content.IActivePivotContentServiceConfig;
import com.qfs.server.cfg.impl.ActivePivotConfig;
import com.qfs.server.cfg.impl.ActivePivotServicesConfig;
import com.qfs.server.cfg.impl.ActivePivotWebSocketServicesConfig;
import com.qfs.server.cfg.impl.ActivePivotXmlaServletConfig;
import com.qfs.server.cfg.impl.ActiveViamRestServicesConfig;
import com.qfs.server.cfg.impl.DatastoreConfig;
import com.qfs.server.cfg.impl.JwtConfig;
import com.qfs.server.cfg.impl.StreamingMonitorConfig;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.monitoring.jmx.impl.JMXEnabler;

/**
 * Generic Spring configuration of the Sandbox ActivePivot server application.
 *
 * <p>
 * This is the entry point for the Spring "Java Config" of the entire application. This is
 * referenced in corresponding WebAppInitializer to bootstrap the application (as per Spring
 * framework principles).
 *
 * <p>
 * We use {@link PropertySource} annotation(s) to define some .properties file(s), whose content
 * will be loaded into the Spring {@link Environment}, allowing some externally-driven configuration
 * of the application.
 *
 * <p>
 * We use {@link Import} annotation(s) to reference additional Spring {@link Configuration} classes,
 * so that we can manage the application configuration in a modular way (split by domain/feature,
 * re-use of core config, override of core config, customized config, etc...).
 *
 * <p>
 * Spring best practices recommends not to have arguments in bean methods if possible. One should
 * rather autowire the appropriate spring configurations (and not beans directly unless necessary),
 * and use the beans from there.
 *
 * @author ActiveViam
 */
@PropertySource(value = { "classpath:autopivot.properties", "classpath:jwt.properties" })
@Configuration
@Import(
value = {
		ActivePivotConfig.class,
		ActivePivotManagerDescriptionConfig.class,
		DatastoreDescriptionConfig.class,
		DatastoreConfig.class,
		ActivePivotBranchPermissionsManagerConfig.class,
		ContentServiceConfig.class,
		SourceConfig.class,
		SecurityConfig.class,
		JwtConfig.class,
		AutoPivotCorsFilterConfig.class,
		
		// ActiveViam Services
		ActivePivotServicesConfig.class,
		ActiveViamRestServicesConfig.class,
		
		// XMLA Servlet
		ActivePivotXmlaServletConfig.class,
		
		// Websocket configuration
		ActivePivotWebSocketServicesConfig.class,
		ContentServerWebSocketServicesConfig.class,

		// Monitoring for the Streaming services
		StreamingMonitorConfig.class,
		
		// ActiveUI resource server configuration
		ActiveUIResourceServerConfig.class
})
public class AutoPivotConfig {

	/** Logger **/
	protected static final Logger LOGGER = Logger.getLogger(AutoPivotConfig.class.getName());

	/** Before anything else we statically initialize the ActiveViam Registry. */
	static {
		AutoPivotGenerator.initRegistry(Arrays.asList("com.av"));
	}

	/** Datastore spring configuration */
	@Autowired
	protected IDatastoreConfig datastoreConfig;

	/** ActivePivot spring configuration */
	@Autowired
	protected IActivePivotConfig apConfig;

	/** JWT spring configuration */
	@Autowired
	protected IJwtConfig jwtConfig;

	/** ActivePivot content service spring configuration */
	@Autowired
	protected IActivePivotContentServiceConfig apCSConfig;

	/** SecurityConfig spring configuration */
	@Autowired
	protected SecurityConfig securityConfig;

	/** ActivePivot Service Config */
	@Autowired
	protected ActivePivotServicesConfig apServiceConfig;

	/**
	 *
	 * Initialize and start the ActivePivot Manager, after performing all the injections into the
	 * ActivePivot plug-ins.
	 *
	 * @see #apManagerInitPrerequisitePluginInjections()
	 * @return void
	 * @throws Exception any exception that occurred during the manager's start up
	 */
	@Bean
	public Void startManager() throws Exception {
		/* ********************************************************************** */
		/* Inject dependencies before the ActivePivot components are initialized. */
		/* ********************************************************************** */
		apManagerInitPrerequisitePluginInjections();

		/* *********************************************** */
		/* Initialize the ActivePivot Manager and start it */
		/* *********************************************** */

		apConfig.activePivotManager().init(null);
		apConfig.activePivotManager().start();

		return null;
	}
	
	@Bean
	public AutoPivotProperties autoPivotProperties() {
		return new AutoPivotProperties();
	}
	
	@Bean
	public DatastoreServiceConfig datastoreServiceConfig(AutoPivotProperties autoPivotProperties,
														 AutoPivotDiscoveryCreator discoveryCreator) {
		return new DatastoreServiceConfig(autoPivotProperties, discoveryCreator);
	}
	
	/** Discover the input data file (CSV separator, column types) */
	@Bean
	public AutoPivotDiscoveryCreator discoveryCreator() {
		return new AutoPivotDiscoveryCreator();
	}

	/**
	 * Enable JMX Monitoring for the Datastore
	 *
	 * @return the {@link JMXEnabler} attached to the datastore
	 */
	@Bean
	public JMXEnabler JMXDatastoreEnabler() {
		return new JMXEnabler(datastoreConfig.datastore());
	}

	/**
	 * Enable JMX Monitoring for ActivePivot Components
	 *
	 * @return the {@link JMXEnabler} attached to the activePivotManager
	 */
	@Bean
	@DependsOn(value = "startManager")
	public JMXEnabler JMXActivePivotEnabler() {
		return new JMXEnabler(apConfig.activePivotManager());
	}

	/**
	 * Enable JMX Monitoring for the Content Service
	 *
	 * @return the {@link JMXEnabler} attached to the content service.
	 */
	@Bean
	public JMXEnabler JMXActivePivotContentServiceEnabler() {
		// to allow operations from the JMX bean
		return new JMXEnabler(
				new DynamicActivePivotContentServiceMBean(
						apCSConfig.activePivotContentService(),
						apConfig.activePivotManager()));
	}

	/**
	 * Health Check Agent bean
	 *
	 * @return the health check agent
	 */
	@Bean(destroyMethod = "stop")
	public HealthCheckAgent healthCheckAgent() {
		return HealthCheckAgent.create(
				datastoreConfig.datastore(),
				apConfig.activePivotManager());
	}

	/**
	 * Extended plugin injections that are required before doing the startup of the ActivePivot
	 * manager.
	 *
	 * @see #startManager()
	 * @throws Exception any exception that occurred during the injection
	 */
	protected void apManagerInitPrerequisitePluginInjections() throws Exception {
		/* ********************************************************* */
		/* Core injections for distributed architecture (when used). */
		/* ********************************************************* */
		// Inject the distributed messenger with security services
		for (Object key : Registry.getExtendedPlugin(IDistributedMessenger.class).keys()) {
			inject(IDistributedMessenger.class, String.valueOf(key), apConfig.contextValueManager());
		}

		// Inject the distributed security manager with security services
		for (Object key : Registry.getExtendedPlugin(IDistributedSecurityManager.class).keys()) {
			inject(IDistributedSecurityManager.class, String.valueOf(key), securityConfig.avUserDetailsService());
		}
	}

}
