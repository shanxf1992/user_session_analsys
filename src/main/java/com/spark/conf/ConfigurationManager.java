package com.spark.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * 1. 在第一次访问组件的时候, 就从对应的 Perperties 文件中读取配置, 并提供外界获取某个配置 key 的 value 的方法
 * 2. 复杂的情况下, 可能需要管理多个 Propterties 或者 xml 类型的配置文件.
 */
public class ConfigurationManager {

    private static Properties props = new Properties();

    static {
        try {

            /**
             * 1. 通过类加载器的方式获取配置文件的输入流
             *      1.1 通过类名.class的方式, 就可以获得类在 JVM 中的Class对象,
             *      1.2 然后通过Class.getClassLoader()获取JVM中加载这个类的类加载器
             *      1.3 然后调用 ClassLoader对象的getResourceAsStream()方法, 就可以用类加载器去加载 类加载路径中的指定的文件
             *      1.4 最终可以获取到一个针对指定文件的一个输入流
             */
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("spark-config.properties");
            //2. 调用 Properties 的 load() 方法, 将配置文件中的key-value格式的配置项, 加载到 Properties 对象中
            props.load(in);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定 key 对应的 value 值
     * @param key 传入的 key值
     * @return 返回对应的 value
     */
    public static String getProperty(String key) {
        return props.getProperty(key);
    }

    /**
     * 获取整数类型的 value 值
     * @param key
     * @return 如果出错, 返回 0
     */
    public static Integer getInteger(String key) {
        try {
            return Integer.parseInt(props.getProperty(key));
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * 获取布尔类型的 value 值
     * @param key
     * @return
     */
    public static Boolean getBoolean(String key) {
        try {
            return Boolean.parseBoolean(props.getProperty(key));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
