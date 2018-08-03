package com.av.autopivot.security.configurer;

import static com.av.autopivot.security.SecurityConstant.ROLE_TECH;
import static com.av.autopivot.security.SecurityConstant.ROLE_USER;
import static com.qfs.QfsWebUtils.url;
import static com.qfs.server.cfg.impl.ActivePivotRestServicesConfig.PING_SUFFIX;
import static com.qfs.server.cfg.impl.ActivePivotRestServicesConfig.REST_API_URL_PREFIX;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.BeanIds;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.authentication.switchuser.SwitchUserFilter;

import com.av.autopivot.security.CookieUtil;
import com.qfs.server.cfg.IActivePivotConfig;

/**
 * Configure security for ActivePivot web services
 *
 * @author ActiveViam
 *
 */
@Configuration
public class ActivePivotSecurityConfigurer extends AWebSecurityConfigurer {

	@Autowired
	protected IActivePivotConfig activePivotConfig;

	/**
	 * Constructor
	 */
	public ActivePivotSecurityConfigurer() {
		super(CookieUtil.COOKIE_NAME);
	}

	@Override
	protected void doConfigure(HttpSecurity http) throws Exception {
		http.authorizeRequests()
				// The order of the matchers matters
				.antMatchers(HttpMethod.OPTIONS, REST_API_URL_PREFIX + "/**")
				.permitAll()
				// The REST ping service is temporarily authenticated (see PIVOT-3149)
				.antMatchers(url(REST_API_URL_PREFIX, PING_SUFFIX))
				.hasAnyAuthority(ROLE_USER, ROLE_TECH)
				// REST services
				.antMatchers(REST_API_URL_PREFIX + "/**")
				.hasAnyAuthority(ROLE_USER)
				// One has to be a user for all the other URLs
				.antMatchers("/**")
				.hasAuthority(ROLE_USER)
				.and()
				.httpBasic()
				// SwitchUserFilter is the last filter in the chain. See FilterComparator class.
				.and()
				.addFilterAfter(activePivotConfig.contextValueFilter(), SwitchUserFilter.class);
	}

	@Bean(name = BeanIds.AUTHENTICATION_MANAGER)
	@Override
	public AuthenticationManager authenticationManagerBean() throws Exception {
		return super.authenticationManagerBean();
	}
}
