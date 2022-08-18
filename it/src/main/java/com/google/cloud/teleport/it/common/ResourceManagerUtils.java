package com.google.cloud.teleport.it.common;

import com.google.common.hash.HashFunction;
import com.google.re2j.Pattern;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import static com.google.common.hash.Hashing.goodFastHash;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class ResourceManagerUtils {

    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS");

    /**
     * Generates a new id string from an existing one.
     *
     * @param id The id string to generate a new id from.
     * @param targetLength The length of the new id to generate. Must be greater than 8.
     */
    public static String generateNewId(String id, int targetLength) {
        if (id.length() <= targetLength) {
            return id;
        }

        if (targetLength <= 8) {
            throw new IllegalArgumentException("targetLength must be greater than 8");
        }

        HashFunction hashFunction = goodFastHash(32);
        String hash = hashFunction.hashUnencodedChars(id).toString();
        return id.substring(0, targetLength - hash.length() - 1) + "-" + hash;
    }

    /**
     * Generates an instance id from a given string.
     *
     * @param baseString The string to generate the id from.
     * @return The instance id string.
     */
    public static String generateInstanceId(String baseString, Pattern illegalChars, int targetLength) {
        checkArgument(baseString.length() != 0, "baseString cannot be empty!");
        String illegalCharsRemoved =
                illegalChars.matcher(baseString.toLowerCase()).replaceAll("-");

        LocalDateTime localDateTime = LocalDateTime.now(ZoneId.of("UTC"));

        // if first char is not a letter, replace with letter, so it doesn't violate spanner's instance
        // naming rules
        if (!Character.isLetter(baseString.charAt(0))) {
            char padding = generatePadding();
            illegalCharsRemoved = padding + illegalCharsRemoved.substring(1);
        }
        String timeAddOn = localDateTime.format(TIME_FORMAT);
        return illegalCharsRemoved.subSequence(0, targetLength - timeAddOn.length() - 1) + "-" + localDateTime.format(TIME_FORMAT);
    }

    /** Generates random letter for padding. */
    public static char generatePadding() {
        Random random = new Random();
        return (char) ('a' + random.nextInt(26));
    }
}
