package com.xumumi.utils.network;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;

/**
 * TCP 客户端
 *
 * @author XUMUMI
 * @version 2.4.2
 * @since 2.4.2
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public final class TcpClient implements Closeable {
    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory.getLogger(TcpClient.class.getName());
    /**
     * 时间间隔
     */
    private static final int TIMEOUT = 3000;

    /**
     * 缓存大小
     */
    private static final int BUFF_SIZE = 64;

    /**
     * 发送线程池
     */
    private static final Executor SEND_EXECUTOR = new ScheduledThreadPoolExecutor(
            1, new BasicThreadFactory.Builder().namingPattern("tcp-send-%d").daemon(true).build()
    );

    /**
     * 接收线程池
     */
    private static final Executor RECEIVE_EXECUTOR = new ScheduledThreadPoolExecutor(
            1, new BasicThreadFactory.Builder().namingPattern("tcp-receive-%d").daemon(true).build()
    );
    /**
     * TCP 客户端对象缓存
     */
    private static final Map<String, TcpClient> TCP_CLIENT_MAP = new HashMap<>(16);
    /**
     * Socket 对象
     */
    @SuppressWarnings("FieldNotUsedInToString")
    private final Socket socket;
    /**
     * 输入流
     */
    @SuppressWarnings("FieldNotUsedInToString")
    private final InputStream inputStream;
    /**
     * 输出流
     */
    @SuppressWarnings("FieldNotUsedInToString")
    private final OutputStream outputStream;

    /**
     * TCP 客户端构造函数
     *
     * @param ip   IP 地址
     * @param port 端口
     * @throws IOException 读写异常
     */
    private TcpClient(final String ip, final Integer port) throws IOException {
        socket = new Socket(ip, port);
        socket.setSoTimeout(TIMEOUT);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
    }

    /**
     * 创建 TCP 客户端
     *
     * @param ip   IP 地址
     * @param port 端口
     * @return TcpClient TCP 客户端
     * @throws IOException 读写异常
     */
    public static TcpClient createTcpClient(final String ip, final Integer port) throws IOException {
        final String key = String.format("%s:%d", ip, port);
        logger.debug(key);
        TcpClient tcpClient = TCP_CLIENT_MAP.get(key);
        if (Objects.isNull(tcpClient)) {
            tcpClient = new TcpClient(ip, port);
            TCP_CLIENT_MAP.put(key, tcpClient);
        }
        return tcpClient;
    }

    /**
     * 收发数据
     *
     * @param ip   IP 地址
     * @param port 端口
     * @param data 数据
     * @return {@link String} 返回数据
     */
    public static byte[] sendAndReceive(final String ip, final Integer port, final byte[] data) {
        byte[] ret = ArrayUtils.EMPTY_BYTE_ARRAY;
        final TcpClient tcpClient;
        try {
            //noinspection resource
            tcpClient = createTcpClient(ip, port);
            tcpClient.sendSync(data);
            ret = tcpClient.receiveBytesSync();
        } catch (final IOException e) {
            logger.error(e.getLocalizedMessage());
        }
        return ret;
    }

    /**
     * 收发字符串
     *
     * @param ip   IP 地址
     * @param port 端口
     * @param data 数据
     * @return {@link String} 返回数据
     */
    public static String sendAndReceive(final String ip, final Integer port, final String data) {
        String ret = StringUtils.EMPTY;
        final TcpClient tcpClient;
        try {
            //noinspection resource
            tcpClient = createTcpClient(ip, port);
            tcpClient.sendSync(data);
            ret = tcpClient.receiveStringSync();
        } catch (final IOException e) {
            logger.error(e.getLocalizedMessage());
        }
        return ret;
    }

    /**
     * 收发字符串
     *
     * @param data 数据
     * @return {@link String} 返回数据
     */
    public String sendAndReceive(final String data) {
        final TcpClient tcpClient;
        sendSync(data);
        final String ret = receiveStringSync();
        logger.debug(ret);
        return ret;
    }

    /**
     * 收发字符串
     *
     * @param data 数据
     * @return {@link Byte}[]
     */
    public byte[] sendAndReceive(final byte[] data) {
        final TcpClient tcpClient;
        sendSync(data);
        final byte[] ret = receiveBytesSync();
        logger.debug(Arrays.toString(ret));
        return ret;
    }

    /**
     * 发送
     *
     * @param data 数据
     */
    public void send(final String data) {
        logger.debug(data);
        SEND_EXECUTOR.execute(() -> sendSync(data));
    }

    /**
     * 发送
     *
     * @param data 数据
     */
    public void send(final byte[] data) {
        logger.debug(Arrays.toString(data));
        SEND_EXECUTOR.execute(() -> sendSync(data));
    }

    /**
     * 同步发送
     *
     * @param data 数据
     */
    public void sendSync(final String data) {
        logger.debug(data);
        sendSync(data.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 同步发送
     *
     * @param data 数据
     */
    public void sendSync(final byte[] data) {
        logger.debug(Arrays.toString(data));
        try {
            outputStream.write(data);
            outputStream.flush();
        } catch (final IOException e) {
            logger.error(e.getLocalizedMessage());
        }
    }

    /**
     * 接收字符串
     *
     * @param callback 回调
     */
    public void receiveString(final Consumer<? super String> callback) {
        logger.debug(callback.toString());
        RECEIVE_EXECUTOR.execute(() ->
        {
            final String data;
            data = receiveStringSync();
            callback.accept(data);
        });
    }

    /**
     * 接收数据
     *
     * @param callback 回调
     */
    public void receiveBytes(final Consumer<byte[]> callback) {
        logger.debug(String.valueOf(callback));
        RECEIVE_EXECUTOR.execute(() ->
        {
            final byte[] data;
            data = receiveBytesSync();
            callback.accept(data);
        });
    }

    /**
     * 同步接收
     *
     * @return {@link String}
     */
    public String receiveStringSync() {
        final byte[] data;
        data = receiveBytesSync();
        logger.debug(Arrays.toString(data));
        return new String(data, StandardCharsets.UTF_8);
    }


    /**
     * 同步接收
     *
     * @return {@link Byte}[]
     */
    public byte[] receiveBytesSync() {
        byte[] ret = ArrayUtils.EMPTY_BYTE_ARRAY;
        try {
            int readLength;
            do {
                final byte[] bytes = new byte[BUFF_SIZE];
                readLength = inputStream.read(bytes);
                ret = ArrayUtils.addAll(ret, bytes);
            } while (BUFF_SIZE <= readLength);
        } catch (final IOException e) {
            logger.error(e.getLocalizedMessage());
        }
        return ret;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @SuppressWarnings("resource")
    @Override
    public void close() throws IOException {
        logger.debug("close");
        inputStream.close();
        outputStream.close();
        socket.close();
        TCP_CLIENT_MAP.remove(String.format("%s:%d", socket.getInetAddress().getHostAddress(), socket.getPort()));
    }

    /**
     * 对象字符串化
     *
     * @return {@link String} 对象的字符串形式
     */
    @Override
    public String toString() {
        final String s = super.toString();
        logger.debug(s);
        return s;
    }
}
