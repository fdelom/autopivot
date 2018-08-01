/*
 * (C) Quartet FS 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.av.autopivot.spring.security;

import static com.av.autopivot.spring.security.SecurityConstant.BASIC_AUTH_BEAN_NAME;
import static com.av.autopivot.spring.security.SecurityConstant.ROLE_ADMIN;
import static com.av.autopivot.spring.security.SecurityConstant.ROLE_CS_ROOT;
import static com.av.autopivot.spring.security.SecurityConstant.ROLE_TECH;
import static com.av.autopivot.spring.security.SecurityConstant.ROLE_USER;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.authentication.configuration.EnableGlobalAuthentication;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManagerBuilder;
import org.springframework.security.provisioning.UserDetailsManager;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;

import com.av.autopivot.spring.config.ui.ActiveUIResourceServerConfig;
import com.av.autopivot.spring.security.configurer.ActivePivotSecurityConfigurer;
import com.av.autopivot.spring.security.configurer.ContentServerSecurityConfigurer;
import com.av.autopivot.spring.security.configurer.JwtSecurityConfigurer;
import com.av.autopivot.spring.security.configurer.VersionSecurityConfigurer;
import com.qfs.jwt.service.IJwtService;
import com.qfs.security.spring.impl.CompositeUserDetailsService;
import com.qfs.server.cfg.IJwtConfig;
import com.quartetfs.biz.pivot.security.IAuthorityComparator;
import com.quartetfs.biz.pivot.security.impl.AuthorityComparatorAdapter;
import com.quartetfs.biz.pivot.security.impl.UserDetailsServiceWrapper;
import com.quartetfs.fwk.ordering.impl.CustomComparator;
import com.quartetfs.fwk.security.IUserDetailsService;

/**
 * Generic implementation for security configuration of a server hosting ActivePivot, or Content
 * server or ActiveMonitor.
 * <p>
 * This class contains methods:
 * <ul>
 * <li>To define authorized users</li>,
 * <li>To enable anomymous user access</li>,
 * <li>To configure the JWT filter</li>,
 * <li>To configure the security for Version service</li>.
 * </ul>
 *
 * @author ActiveViam
 */
@Import(value = {
	 ActiveUIResourceServerConfig.class,		 
	 JwtSecurityConfigurer.class,
	 VersionSecurityConfigurer.class,
	 ContentServerSecurityConfigurer.class,
	 ActivePivotSecurityConfigurer.class
})
@EnableGlobalAuthentication
@EnableWebSecurity
@Configuration
public abstract class SecurityConfig {

	@Autowired
	protected IJwtConfig jwtConfig;
	
	@Autowired
	protected PasswordEncoder passwordEncoder;

	/**
	 * Returns the default {@link AuthenticationEntryPoint} to use
	 * for the fallback basic HTTP authentication.
	 *
	 * @return The default {@link AuthenticationEntryPoint} for the
	 *         fallback HTTP basic authentication.
	 */
	@Bean(name=BASIC_AUTH_BEAN_NAME)
	public AuthenticationEntryPoint basicAuthenticationEntryPoint() {
		return new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED);
	}

	@Autowired
	public void configureGlobal(AuthenticationManagerBuilder auth) throws Exception {
		auth
				.eraseCredentials(false)
				// Add an LDAP authentication provider instead of this to support LDAP
				.userDetailsService(userDetailsService()).and()
				// Required to allow JWT
				.authenticationProvider(jwtConfig.jwtAuthenticationProvider());
	}
	
	/**
	 * Required with Spring Security 5.5
	 * @return PasswordEncoder (default is based on BCrypt)
	 */
	@Bean
	public PasswordEncoder passwordEncoder() {
		return PasswordEncoderFactories.createDelegatingPasswordEncoder();
	}
	
	/**
	 * User details service wrapped into an ActiveViam interface.
	 * <p>
	 * This bean is used by {@link ActiveMonitorPivotExtensionServiceConfiguration}
	 *
	 * @return a user details service
	 */
	@Bean
	public IUserDetailsService avUserDetailsService() {
		return new UserDetailsServiceWrapper(userDetailsService());
	}
	
	/**
	 * [Bean] Create the users that can access the application
	 *
	 * @return {@link UserDetailsService user data}
	 */
	@Bean
	public UserDetailsService userDetailsService() {
		InMemoryUserDetailsManagerBuilder b = new InMemoryUserDetailsManagerBuilder()
				.passwordEncoder(passwordEncoder)
				.withUser("admin")
				.password(passwordEncoder.encode("admin"))
				.authorities(ROLE_USER, ROLE_ADMIN, ROLE_CS_ROOT)
			.and()
				.withUser("user")
				.password(passwordEncoder.encode("user"))
				.authorities(ROLE_USER).and();

		return new CompositeUserDetailsService(Arrays.asList(b.build(), technicalUserDetailsService()));
	}

	/**
	 * Creates a technical user to allow ActivePivot to connect
	 * to the content server.
	 *
	 * @return {@link UserDetailsService user data}
	 */
	protected UserDetailsManager technicalUserDetailsService() {
		return new InMemoryUserDetailsManagerBuilder()
				.withUser("pivot")
				.password(passwordEncoder.encode("pivot"))
				.authorities(ROLE_TECH, ROLE_CS_ROOT)
			.and()
				.build();
	}
	
	/**
	 * [Bean] Comparator for user roles
	 * <p>
	 * Defines the comparator used by:
	 * </p>
	 * <ul>
	 *   <li>com.quartetfs.biz.pivot.security.impl.ContextValueManager#setAuthorityComparator(IAuthorityComparator)</li>
	 *   <li>{@link IJwtService}</li>
	 * </ul>
	 * @return a comparator that indicates which authority/role prevails over another. <b>NOTICE -
	 *         an authority coming AFTER another one prevails over this "previous" authority.</b>
	 *         This authority ordering definition is essential to resolve possible ambiguity when,
	 *         for a given user, a context value has been defined in more than one authority
	 *         applicable to that user. In such case, it is what has been set for the "prevailing"
	 *         authority that will be effectively retained for that context value for that user.
	 */
	@Bean
	public IAuthorityComparator authorityComparator() {
		final CustomComparator<String> comp = new CustomComparator<>();
		comp.setFirstObjects(Arrays.asList(ROLE_USER));
		comp.setLastObjects(Arrays.asList(ROLE_ADMIN));
		return new AuthorityComparatorAdapter(comp);
	}
}
