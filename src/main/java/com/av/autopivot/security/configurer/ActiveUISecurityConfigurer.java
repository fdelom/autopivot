package com.av.autopivot.security.configurer;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;

import com.av.autopivot.config.ui.ActiveUIResourceServerConfig;

/**
 * Configuration for ActiveUI.
 *
 * @author ActiveViam
 * @see HttpStatusEntryPoint
 */
@Configuration
@Order(1)
public class ActiveUISecurityConfigurer extends AWebSecurityConfigurer {

	@Override
	protected void doConfigure(HttpSecurity http) throws Exception {
		// Permit all on ActiveUI resources and the root (/) that redirects to ActiveUI index.html.
		final String pattern = "^(.{0}|\\/|\\/" + ActiveUIResourceServerConfig.NAMESPACE + "(\\/.*)?)$";
		http
				// Only theses URLs must be handled by this HttpSecurity
				.regexMatcher(pattern)
				.authorizeRequests()
				// The order of the matchers matters
				.regexMatchers(HttpMethod.OPTIONS, pattern)
				.permitAll()
				.regexMatchers(HttpMethod.GET, pattern)
				.permitAll();

		// Authorizing pages to be embedded in iframes to have ActiveUI in ActiveMonitor UI
		http.headers().frameOptions().disable();
	}

}
