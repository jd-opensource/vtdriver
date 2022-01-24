package com.jd.jdbc.util;

import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class NetUtil {
    private static final Log log = LogFactory.getLog(NetUtil.class);

    private static String localAddress = null;

    static {
        try {
            localAddress = getLocalHostAddress().getHostAddress();
        } catch (UnknownHostException | SocketException e) {
            log.error("getLocalAddr error! causeby:%s" + e.getMessage());
        }
    }

    public static String getLocalAdder() {
        return localAddress;
    }

    private static InetAddress getLocalHostAddress() throws UnknownHostException, SocketException {
        Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        InetAddress ip = null;
        while (allNetInterfaces.hasMoreElements()) {
            NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
            Enumeration addresses = netInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                ip = (InetAddress) addresses.nextElement();
                if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && !ip.getHostAddress().contains(":")) {
                    if (ip instanceof Inet4Address) {
                        return ip;
                    }
                }
            }
        }
        InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
        if (jdkSuppliedAddress == null) {
            throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
        }
        return jdkSuppliedAddress;
    }

    /**
     * 向指定 URL 发送POST方法的请求
     *
     * @param url  发送请求的 URL
     * @param json 请求参数 json格式
     * @return 所代表远程资源的响应结果
     */
    public static String sendPostQuery(String url, String json) {
        PrintWriter out = null;
        BufferedReader in = null;
        StringBuilder result = new StringBuilder();
        try {
            URL realUrl = new URL(url);
            URLConnection conn = realUrl.openConnection();
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent", "vtdriver");
            conn.setRequestProperty("Content-Type", "application/json");

            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.connect();

            out = new PrintWriter(conn.getOutputStream());
            out.print(json);
            out.flush();
            InputStream instream = conn.getInputStream();
            if (instream != null) {
                in = new BufferedReader(new InputStreamReader(instream));
                String line;
                while ((line = in.readLine()) != null) {
                    result.append(line);
                }
            }
        } catch (Exception e) {
            log.error("sendPostQuery error! causeby:" + e.getMessage());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                log.error("close error!", ex);
            }
        }
        return result.toString();
    }
}
