package com.ctrip.framework.kbear.rest;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

import org.springframework.boot.autoconfigure.http.HttpMessageConverters;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.accept.ContentNegotiationStrategy;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import com.google.protobuf.util.JsonFormat.Printer;

/**
 * @author koqizhao
 *
 * Sep 21, 2018
 */
@Configuration
public class RestConfig implements WebMvcConfigurer {

    public static final String APPLICATION_PROTOBUF_VALUE = "application/x-protobuf";

    @Override
    public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
        configurer.defaultContentType(MediaType.APPLICATION_JSON)
                .strategies(Arrays.asList(new ContentNegotiationStrategy() {
                    @Override
                    public List<MediaType> resolveMediaTypes(NativeWebRequest webRequest)
                            throws HttpMediaTypeNotAcceptableException {
                        MediaType mediaType = MediaType.APPLICATION_JSON;
                        String accept = webRequest.getHeader(HttpHeaders.ACCEPT);
                        if (accept == null)
                            return Arrays.asList(mediaType);

                        switch (accept) {
                            case APPLICATION_PROTOBUF_VALUE:
                                mediaType = ProtobufHttpMessageConverter.PROTOBUF;
                                break;
                            case MediaType.APPLICATION_JSON_VALUE:
                                mediaType = MediaType.APPLICATION_JSON;
                                break;
                            default:
                                mediaType = MediaType.APPLICATION_JSON;
                                break;
                        }

                        return Arrays.asList(mediaType);
                    }
                }));
    }

    @Bean
    public HttpMessageConverters customConverters() {
        try {
            HttpMessageConverter<Message> converter = newCustomProtobufMessageConverter();
            return new HttpMessageConverters(converter);
        } catch (Exception e) {
            throw new IllegalStateException("cannot construct custom protobuf converter", e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private ProtobufHttpMessageConverter newCustomProtobufMessageConverter() throws Exception {
        Constructor[] constructors = ProtobufHttpMessageConverter.class.getDeclaredConstructors();
        Constructor requiredConstructor = null;
        for (Constructor constructor : constructors) {
            if (constructor.getParameterTypes().length == 2) {
                constructor.setAccessible(true);
                requiredConstructor = constructor;
                break;
            }
        }

        Class[] classes = ProtobufHttpMessageConverter.class.getDeclaredClasses();
        Class requiredClass = null;
        for (Class clazz : classes) {
            if (clazz.getSimpleName().equals("ProtobufJavaUtilSupport")) {
                requiredClass = clazz;
                break;
            }
        }

        Constructor pbUtilSupportConstructor = requiredClass.getConstructor(Parser.class, Printer.class);
        pbUtilSupportConstructor.setAccessible(true);

        Parser parser = JsonFormat.parser().ignoringUnknownFields();
        Printer printer = JsonFormat.printer().includingDefaultValueFields().preservingProtoFieldNames()
                .omittingInsignificantWhitespace();
        Object support = pbUtilSupportConstructor.newInstance(parser, printer);
        return (ProtobufHttpMessageConverter) requiredConstructor.newInstance(support, null);
    }

}
