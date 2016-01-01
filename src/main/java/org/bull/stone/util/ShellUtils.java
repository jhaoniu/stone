package org.bull.stone.util;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-7 下午7:00
 */
public class ShellUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShellUtils.class);

    public static boolean executeCommand(String[] args) {
        boolean isSuccessed = true;
        try {
            Process process = Runtime.getRuntime().exec(args);
            process.waitFor();

            BufferedReader results = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));
            String s;
            while ((s = results.readLine()) != null)
                LOGGER.info(s);
            BufferedReader errors = new BufferedReader(
                    new InputStreamReader(process.getErrorStream()));
            while ((s = errors.readLine()) != null) {
                LOGGER.info(s);
                isSuccessed = false;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        if (!isSuccessed)
            throw new RuntimeException("Errors executing " + Joiner.on(" ").join(args)
            );
        return isSuccessed;
    }

    public static void main(String[] args) {
        ShellUtils.executeCommand(new String[]{"/bin/bash", "-c", "cp /opt/data/* /opt/tmp"});
    }
}
