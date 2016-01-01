package org.bull.stone.util;

import java.io.File;

/**
 * @author jhaoniu
 * @description
 * @date 15-12-6 下午9:02
 */
public class FileUtils {
    public static boolean deleteDir(File dir) {

        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }


    public static boolean deleteChildrenDir(File dir) {

        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean deleteChildrenFiles(File dir) {

        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = new File(dir, children[i]).delete();
                if (!success) {
                    return false;
                }
            }
        }
        return true;
    }
}
