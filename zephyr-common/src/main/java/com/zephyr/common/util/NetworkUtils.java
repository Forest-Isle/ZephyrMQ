package com.zephyr.common.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public final class NetworkUtils {

    private static volatile String localAddress;

    public static String getLocalAddress() {
        if (localAddress != null) {
            return localAddress;
        }

        synchronized (NetworkUtils.class) {
            if (localAddress != null) {
                return localAddress;
            }

            localAddress = doGetLocalAddress();
            return localAddress;
        }
    }

    private static String doGetLocalAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.isLoopback() || networkInterface.isVirtual() || !networkInterface.isUp()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address.isLoopbackAddress() || !address.isSiteLocalAddress()) {
                        continue;
                    }

                    String hostAddress = address.getHostAddress();
                    if (hostAddress != null && hostAddress.indexOf(':') == -1) {
                        return hostAddress;
                    }
                }
            }
        } catch (SocketException e) {
            // Fallback to localhost
        }

        return "127.0.0.1";
    }

    public static String formatAddress(String host, int port) {
        return host + ":" + port;
    }

    public static String extractHost(String address) {
        int index = address.lastIndexOf(':');
        return index > 0 ? address.substring(0, index) : address;
    }

    public static int extractPort(String address, int defaultPort) {
        int index = address.lastIndexOf(':');
        if (index > 0) {
            try {
                return Integer.parseInt(address.substring(index + 1));
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return defaultPort;
    }

    private NetworkUtils() {
        throw new IllegalStateException("Utility class");
    }
}