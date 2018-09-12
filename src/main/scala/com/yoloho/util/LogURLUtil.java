/**
 * FileName: LogURLUtil
 * Author:   bigdata03
 * Date:     2018/9/3 17:18
 * Description:
 * History:
 */
package com.yoloho.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author bigdata03
 * @create 2018/9/3
 * @since 1.0.0
 */
public class LogURLUtil {

    private static Pattern qPattern = Pattern.compile("[?]");
    private static Pattern aPattern = Pattern.compile("[&]");
    private static Pattern ePattern = Pattern.compile("[=]");

    /**
     * 抽取URL中的地址
     * @param url URL
     *            /group/group/searchall_v4
     * @return
     */
    public static String getAddress(String url) {
        if (url == null || url.isEmpty()) {
            return "";
        }
        url = url.trim();
        if (url.contains("?")) {
            String[] fields = qPattern.split(url);
            if (fields.length == 2 && (fields[0].charAt(0) == '/' || fields[0].startsWith("http"))) {
                return fields[0];
            }
        }
        return "";
    }

    /**
     * 抽取URL中的参数字符串
     * @param url URL
     * @return
     */
    public static String getParamString(String url) {
        if (url == null || url.isEmpty()) {
            return "";
        }
        url = url.trim();//去掉字符串左右两边的空格
        if (url.contains("?")) {
            String[] fields = qPattern.split(url);
            if (fields.length == 2 && fields[1].contains("=")) {
                return fields[1];//网址?以及后面的参数
            }
        } else if (url.contains("=")) {
            return url;//整个网址
        }
        return "";//空
    }


    /**
     * 抽取URL中的参数键值对
     * @param url URL
     * @return
     */
    public static Map<String, String> getParamMap(String url) throws UnsupportedEncodingException {
        Map<String, String> result = new HashMap<String, String>();
        String params = getParamString(url);
        if (params.equals("")) {
            return result;//返回空
        }
        String[] fields = aPattern.split(params);
        for (String field : fields) {
            String[] pair = ePattern.split(field);
            if (pair.length == 2) {
                result.put(pair[0], URLDecoder.decode(pair[1].replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8"));
            }
        }
        return result;
    }

    /**
     * 抽取URL中token内的UID
     * @param url URL
     * @return
     */
    public static long getUIDFromURLToken(String url) {
        int startIndex = url.indexOf("token=");
        if (startIndex < 0) {
            return -1L;
        }
        int endIndex = url.indexOf('-', startIndex);
        if (endIndex < 0) {
            return -1L;
        }
        String uid = url.substring(startIndex + 6, endIndex);
        if (uid.length() > 0 && StringUtils.isNumeric(uid)) {
            return NumberUtils.createLong(uid);
        }
        return -1L;
    }


}