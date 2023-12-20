package com.pabu5h.evs2.evs2helper.auth;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

/*
NOTE: Remember to add the following annotation to the main class of the application
@EnableConfigurationProperties(RsaKeyProperties.class)
 */
@ConfigurationProperties(prefix = "rsa")
public record RsaKeyProperties(RSAPublicKey publicKey, RSAPrivateKey privateKey){
}
