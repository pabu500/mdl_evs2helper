package org.pabuff.evs2helper.cloud;

import lombok.Getter;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

import java.util.Map;
import java.util.Optional;

@Getter
public enum AclUseCase {
    PRIVATE(ObjectCannedACL.PRIVATE),
    PUBLIC_READ(ObjectCannedACL.PUBLIC_READ),
    AUTHENTICATED_READ(ObjectCannedACL.AUTHENTICATED_READ),
    BUCKET_OWNER_READ(ObjectCannedACL.BUCKET_OWNER_READ),
    BUCKET_OWNER_FULL_CONTROL(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);

    private final ObjectCannedACL cannedACL;

    AclUseCase(ObjectCannedACL objectCannedACL) {
        this.cannedACL = objectCannedACL;
    }

    public static Optional<AclUseCase> fromString(String name) {
        try {
            return Optional.of(AclUseCase.valueOf(name.toUpperCase()));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public static ObjectCannedACL fromMap(Map<String, Object> s3) {

        if (s3 == null) {
            return PRIVATE.getCannedACL();
        }

        Object aclValue = s3.get("acl");

        if (aclValue instanceof ObjectCannedACL) {
            return (ObjectCannedACL) aclValue;
        }
        if (aclValue instanceof String) {
            return fromString((String) aclValue)
                    .map(AclUseCase::getCannedACL)
                    .orElse(PRIVATE.getCannedACL());
        }
        return PRIVATE.getCannedACL();
    }

}
