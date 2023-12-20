package com.pabu5h.evs2.evs2helper.auth;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class AuthHelper {
    Logger logger = Logger.getLogger(AuthHelper.class.getName());

    @Autowired
    RsaKeyProperties rsaKeyProperties;

    public String genToken(Map<String, String> claims) {
        Instant now = Instant.now();
        return Jwts.builder()
            .setIssuedAt(Date.from(now))
            .setClaims(claims)
            .signWith(rsaKeyProperties.privateKey())
            .compact();
    }

    public Map<String, Object> extractClaims(String token) {
        Claims claims = extractAllClaims(token);
        if(claims.containsKey("error")){
            return Map.of("error", claims.get("error"));
        }
        return Map.of(
            "applicant", claims.get("applicant"),
            "endpoint", claims.get("endpoint"),
            "cred", claims.get("cred")
        );
    }

    private Claims extractAllClaims(String jwtToken){
        JwtParser jwtParser = Jwts.parserBuilder()
                .setSigningKey(rsaKeyProperties.publicKey())
                .build();
        try {
            return jwtParser.parseClaimsJws(jwtToken).getBody();
        } catch (ExpiredJwtException e) {
            logger.info("ExpiredJwtException: " + e.getMessage());
            return e.getClaims();
        }
    }
}
