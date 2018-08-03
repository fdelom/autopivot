package com.av.autopivot.security.configurer;

import static com.av.autopivot.security.SecurityConstant.ROLE_USER;

import javax.servlet.Filter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.context.SecurityContextPersistenceFilter;

import com.qfs.security.cfg.ICorsFilterConfig;
import com.qfs.server.cfg.IJwtConfig;
import com.qfs.servlet.handlers.impl.NoRedirectLogoutSuccessHandler;

/**
 * Common web security configuration for {@link HttpSecurity}.
 *
 * @author ActiveViam
 */
public abstract class AWebSecurityConfigurer extends WebSecurityConfigurerAdapter {
	/** Set to true to allow anonymous access */
	public static final boolean useAnonymous = false;
	
	/** {@code true} to enable the logout URL */
	protected final boolean logout;
	/** The name of the cookie to clear */
	protected final String cookieName;

	@Autowired
	protected Environment env;

	@Autowired
	protected ApplicationContext context;

	/**
	 * This constructor does not enable the logout URL
	 */
	public AWebSecurityConfigurer() {
		this(null);
	}

	/**
	 * This constructor enables the logout URL
	 *
	 * @param cookieName the name of the cookie to clear
	 */
	public AWebSecurityConfigurer(String cookieName) {
		this.logout = cookieName != null;
		this.cookieName = cookieName;
	}

	@Override
	protected final void configure(final HttpSecurity http) throws Exception {
		Filter jwtFilter = context.getBean(IJwtConfig.class).jwtFilter();
		Filter corsFilter = context.getBean(ICorsFilterConfig.class).corsFilter();

		http
				// As of Spring Security 4.0, CSRF protection is enabled by default.
				.csrf().disable()
				// Configure CORS
				.addFilterBefore(corsFilter, SecurityContextPersistenceFilter.class)
				// To allow authentication with JWT (Required for ActiveUI)
				.addFilterAfter(jwtFilter, SecurityContextPersistenceFilter.class);

		if (logout) {
			// Configure logout URL
			http.logout()
					.permitAll()
					.deleteCookies(cookieName)
					.invalidateHttpSession(true)
					.logoutSuccessHandler(new NoRedirectLogoutSuccessHandler());
		}

		if (useAnonymous) {
			// Handle anonymous users. The granted authority ROLE_USER
			// will be assigned to the anonymous request
			http.anonymous().principal("guest").authorities(ROLE_USER);
		}

		doConfigure(http);
	}

	/**
	 * @see #configure(HttpSecurity)
	 */
	protected abstract void doConfigure(HttpSecurity http) throws Exception;

}
