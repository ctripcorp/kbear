package com.ctrip.framework.kbear.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.mydotey.java.ObjectExtension;
import org.mydotey.java.StringExtension;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

/**
 * @author koqizhao
 *
 * Jan 10, 2019
 */
public interface CatProxy {

    static <T> T newInstance(T instance, Class<?> interfaceType) {
        ObjectExtension.requireNonNull(instance, "instance");
        ObjectExtension.requireNonNull(interfaceType, "interfaceType");

        return newInstance(instance, interfaceType, null);
    }

    static <T> T newInstance(T instance, Class<?> interfaceType, String transactionName) {
        ObjectExtension.requireNonNull(instance, "instance");
        ObjectExtension.requireNonNull(interfaceType, "interfaceType");

        return newInstance(instance, interfaceType, null, transactionName);
    }

    @SuppressWarnings("unchecked")
    static <T> T newInstance(T instance, Class<?> interfaceType, String transactionType, String transactionName) {
        ObjectExtension.requireNonNull(instance, "instance");
        ObjectExtension.requireNonNull(interfaceType, "interfaceType");

        return (T) Proxy.newProxyInstance(CatProxy.class.getClassLoader(), new Class[] { interfaceType },
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        String type = StringExtension.isBlank(transactionType) ? instance.getClass().getSimpleName()
                                : transactionType;
                        String name = StringExtension.isBlank(transactionName) ? method.getName() : transactionName;
                        if (!StringExtension.isBlank(transactionName) && StringExtension.isBlank(transactionType))
                            type += "." + method.getName();
                        Transaction transaction = Cat.newTransaction(type, name);
                        try {
                            Object result = method.invoke(instance, args);
                            transaction.setStatus(Transaction.SUCCESS);
                            return result;
                        } catch (Throwable e) {
                            if (e instanceof InvocationTargetException && e.getCause() != null)
                                e = e.getCause();
                            transaction.setStatus(e.getClass().getSimpleName());
                            Cat.logError(e);
                            throw e;
                        } finally {
                            transaction.complete();
                        }
                    }
                });
    }

}
