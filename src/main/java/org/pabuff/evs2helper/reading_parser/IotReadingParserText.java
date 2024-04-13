package org.pabuff.evs2helper.reading_parser;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.logging.Logger;

public abstract class IotReadingParserText<T> {
    protected Map<String, String> lookupTable;
    public abstract T parse(String message);
    public String preClean(String message) {
        String cleanedMessage = message;
        //replace line breaks with ';' (semicolon)
//        cleanedMessage = cleanedMessage.replace("\r\n", ";");
        cleanedMessage = cleanedMessage.replace("\n", ";").replace("\r", ";");
        //replace double semicolons with single semicolon
        cleanedMessage = cleanedMessage.replace(";;", ";");
        //replace tabs with ' ' (space)
        cleanedMessage = cleanedMessage.replace("\t", " ");
        //replace spaces with ',' (comma)
        cleanedMessage = cleanedMessage.replace(" ", ",");

        return cleanedMessage;
    }
    protected void setPropertyValue(T dto,
                                    String propertyCode, String value,
                                    Map<String, String> lookupTable,
                                    Logger logger) {
        String fieldName = lookupTable.get(propertyCode);
        if (fieldName == null) return;

        String methodName = "set" + capitalize(fieldName);

        try {
            Method method;
            if ("setDt".equals(methodName)) {
                method = dto.getClass().getMethod(methodName, LocalDateTime.class);
                method.invoke(dto, LocalDateTime.parse(value));
            } else {
                // NOTE: Must use Double.class instead of double.class,
                // as Double is the type of the setter method parameter in the Dto,
                // otherwise it will throw NoSuchMethodException
                method = dto.getClass().getMethod(methodName, Double.class);
                method.invoke(dto, Double.parseDouble(value));
            }
        } catch (Exception e) {
            logger.severe(e.getMessage());
        }
    }
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }
}
