package org.pabuff.evs2helper.auth;

import com.pabu5h.evs2.dto.SvcClaimDto;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class AuthHelper {
    Logger logger = Logger.getLogger(AuthHelper.class.getName());

    @Value("${service.name}")
    private String svcName;
    @Value("${auth.path}")
    private String authPath;
    @Value("${auth.ept.validate_svc_token}")
    private String validateSvcTokenEpt;
    @Value("${auth.dev_token}")
    private String devToken;

    @Autowired
    RestTemplate restTemplate;
    @Autowired
    RsaKeyProperties rsaKeyProperties;

    public Map<String, String> checkAuth(String authHeader, SvcClaimDto svcClaimDto) {
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            return Collections.singletonMap("error", "Missing or invalid Authorization header.");
        }
        String jwtToken = authHeader.substring(7);
        return validateSvcToken(jwtToken, svcClaimDto);
    }
    private Map<String, String> validateSvcToken(String token, SvcClaimDto svcClaimDto) {
        if(devToken!=null) {
            if (!devToken.isEmpty()) {
                if (token.equals(devToken)) {
                    return Collections.singletonMap("isValid", "true");
                }
            }
        }

        String validateSvcTokenUrl = authPath + validateSvcTokenEpt;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer " + token);

        HttpEntity<SvcClaimDto> requestEntity = new HttpEntity<>(svcClaimDto, headers);

        try {
            ResponseEntity<?> response = restTemplate.postForEntity(validateSvcTokenUrl, requestEntity, Map.class);

            if(response.getStatusCode() == HttpStatus.OK){
                if(response.getBody() instanceof Map<?, ?>) {
//                    return (Boolean) ((Map<?, ?>) response.getBody()).get("isValid");
                    return (Map<String, String>) response.getBody();
                }else {
                    return Collections.singletonMap("error", "Invalid response from auth service 1");
                }
            }else {
                return Collections.singletonMap("error", "Invalid response from auth service 2");
            }
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
    }

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
