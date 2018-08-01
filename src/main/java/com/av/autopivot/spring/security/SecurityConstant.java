package com.av.autopivot.spring.security;

import com.qfs.content.service.IContentService;

public class SecurityConstant {
	public static final String BASIC_AUTH_BEAN_NAME = "basicAuthenticationEntryPoint";

	/** Admin user */
	public static final String ROLE_ADMIN = "ROLE_ADMIN";
	
	/** Standard user role */
	public static final String ROLE_USER = "ROLE_USER";

	/** Role for technical components */
	public static final String ROLE_TECH = "ROLE_TECH";
	
	/** Content Server Root role */
	public static final String ROLE_CS_ROOT = IContentService.ROLE_ROOT;
}
