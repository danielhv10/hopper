/*
 * Copyright 2019 Hopper
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public final class PropertiesLoader {

    public static final String PROPERTIES_FILE_NAME = "application.properties";
    public static final char ENVIRONMENT_VARIABLE_WORD_SEPARATOR = '_';

    public static Properties loadProperties(final HashSet<String> expectedProperties, char wordSeparator, Logger... loggers) {
        Properties properties = new Properties();
        //Recorremos las variables de entorno
        Arrays.stream(loggers).forEach(l -> l.info(PropertiesLoader.class.toString() + ": Getting available properties from environment variables"));
        System.getenv().forEach((k, v) -> {
            String auxName = k.replace(ENVIRONMENT_VARIABLE_WORD_SEPARATOR, wordSeparator);
            if ((expectedProperties.contains(auxName))) {
                properties.put(auxName, v);
            }
        });

        Arrays.stream(loggers).forEach(l -> l.info(PropertiesLoader.class.toString() + ": Getting available properties from file if located in main path"));

        Properties auxPropertiesFile = new Properties();
        try (InputStream is1 = new FileInputStream("." + File.separator + PROPERTIES_FILE_NAME)){
            auxPropertiesFile.load(is1);
            Arrays.stream(loggers).forEach(l -> l.info(PropertiesLoader.class.toString() + ": Properties file in main path was found"));
        } catch (Exception e) {
            Arrays.stream(loggers).forEach(l -> l.info(PropertiesLoader.class.toString() + ": Properties file were not found, trying from resources"));
            try (InputStream is2 = ClassLoader.getSystemResourceAsStream("application.properties")) {
                auxPropertiesFile.load(is2);
                Arrays.stream(loggers).forEach(l -> l.info(PropertiesLoader.class.toString() + ": Properties file was found in resources"));
            }
            catch (IOException e1) {
                Arrays.stream(loggers).forEach(l -> l.info(PropertiesLoader.class.toString() + ": No properties file found"));
                e1.printStackTrace();
            }
        }

        auxPropertiesFile.forEach((k, v) -> {
            if (!properties.containsKey(k)) {
                properties.put(k, v);
            }
        });

        return properties;
    }
}
