package com.av.autopivot.spring.security.configurer;

import static com.av.autopivot.spring.security.SecurityConstant.ROLE_USER;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;

import com.qfs.QfsWebUtils;
import com.qfs.content.cfg.impl.ContentServerRestServicesConfig;
import com.qfs.pivot.servlet.impl.ContextValueFilter;
import com.qfs.server.cfg.content.IActivePivotContentServiceConfig;

/**
 * Only required if the content service is exposed.
 * <p>
 * Separated from {@link ActivePivotServerSecurityConfig.ActivePivotSecurityConfigurer} to skip the {@link ContextValueFilter}.
 * <p>
 * Must be done before ActivePivotSecurityConfigurer (because they match common URLs)
 *
 * @see IActivePivotContentServiceConfig
 */
@Configuration
@Order(4)
public class ContentServerSecurityConfigurer extends AWebSecurityConfigurer {

	@Override
	protected void doConfigure(HttpSecurity http) throws Exception {
		final String url = ContentServerRestServicesConfig.NAMESPACE;
		http
			// Only theses URLs must be handled by this HttpSecurity
			.antMatcher(url + "/**")
			.authorizeRequests()
			// The order of the matchers matters
			.antMatchers(
				HttpMethod.OPTIONS,
				QfsWebUtils.url(ContentServerRestServicesConfig.REST_API_URL_PREFIX + "**"))
			.permitAll()
			.antMatchers(url + "/**")
			.hasAuthority(ROLE_USER)
			.and()
			.httpBasic();
	}

}
