package com.datasift.dropwizard.util;

import io.dropwizard.util.Duration;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

/**
 * Tests for {@link Classes}.
 */
public class ClassesTest {

    @Test
    public void resolveByteArrayArgument() {
        final byte[] test = "test".getBytes();
        assertThat("byte array parsed as single argument",
                Classes.resolveVarArgs(test),
                is(new Object[]{test}));
    }

    @Test
    public void resolveStringArrayArgument() {
        final String[] test = new String[] { "test", "nolliston" };
        assertThat("String array parsed as single element",
                Classes.resolveVarArgs(test),
                is(new Object[]{test}));
    }

    @Test
    public void resolveHeterogeneousArgsAsMultiple() {
        assertThat("heterogeneous argument list parsed as multiple args",
                Classes.resolveVarArgs("test", 123, "test".getClass()),
                is(new Object[]{"test", 123, "test".getClass()}));
    }

    @Test
    public void resolveHomogeneousArgsAsMultiple() {
        assertThat("homogeneous argument list parsed as multiple args",
                Classes.resolveVarArgs("1", "2", "3"),
                is(new Object[]{"1","2","3"}));
    }

    @Test
    public void resolveArgListAsMultiple() {
        final Object[] test = new Object[] { "test", "nolliston" };
        assertThat("Object-arrayed argument list parsed as multiple args",
                Classes.resolveVarArgs(test),
                is(test));
    }

    @Test
    public void resolveArgListNull() {
        assertThat("null arg is single-null arg list",
                Classes.resolveVarArgs(null),
                is(new Object[] { null }));
        assertThat("arg list of nulls is multiple arg list",
                Classes.resolveVarArgs(null, null),
                is(new Object[] { null, null }));
    }

    @Test
    public void classesOfPrimitives() {
        assertThat("generates Class list for primitive arguments",
                Classes.of(123, 123L, true),
                is(new Class[] { Integer.class, Long.class, Boolean.class }));
    }

    @Test
    public void classesOfObjects() {
        assertThat("generates Class list for reference arguments",
                Classes.of("test", String.class, new Object(), 123),
                is(new Class[] { String.class, Class.class, Object.class, Integer.class }));
    }

    @Test
    public void classesOfNulls() {
        assertThat("generates Null Class for null argument",
                Classes.of(null),
                is(new Class[] { Classes.Null.class }));
        assertThat("generates Null Class for null arguments",
                Classes.of(null, null),
                is(new Class[] { Classes.Null.class, Classes.Null.class }));
    }

    @Test
    public void newInstanceString() throws Exception {
        assertThat("instantiates String via reflection",
                Classes.newInstance(String.class, "test"),
                is("test"));
    }

    @Test
    public void newInstanceStringMultipleArgs() throws Exception {
        assertThat("instantiates String via reflection with multiple args",
                Classes.newInstance(String.class, "test".getBytes(), "UTF-8"),
                is("test"));
    }

    @Test
    public void newInstanceFromTemplate() throws Exception {
        final String template = "test";
        assertThat("instantiates a String given a template",
                Classes.newInstanceFrom(template, template),
                is("test"));
        assertThat("new String isn't identical to template",
                Classes.newInstanceFrom(template, template),
                is(not(sameInstance(template))));
        assertThat("new String doesn't copy template's content",
                Classes.newInstanceFrom(template, "nolliston"),
                is(not(template)));
    }

    @Test
    public void unsafeNewInstance() throws Exception {
        assertThat("instantiates public constructors",
                Classes.unsafeNewInstance(String.class, "test"),
                is("test"));
        assertThat("instantiates private constructors",
                Classes.unsafeNewInstance(Duration.class, 1347387660113L, TimeUnit.MILLISECONDS),
                is(Duration.milliseconds(1347387660113L)));
    }

    @Test
    public void unsafeNewFromTemplate() throws Exception {
        assertThat("instantiates public constructors",
                Classes.unsafeNewInstanceFrom("test", "nolliston"),
                is("nolliston"));
        assertThat("instantiates private constructors",
                Classes.unsafeNewInstanceFrom(Duration.days(1), 10, TimeUnit.SECONDS),
                is(Duration.seconds(10)));
    }

    @Test
    public void applicableConstructorNullary() throws NoSuchMethodException {
        assertThat("locates nullary constructor",
                Classes.getApplicableConstructor(ArrayList.class),
                is(ArrayList.class.getDeclaredConstructor()));
    }

    @Test
    public void applicableConstructorSimple() throws NoSuchMethodException {
        assertThat("locates exact match for constructor",
                Classes.getApplicableConstructor(String.class, byte[].class),
                is(any(Constructor.class)));
    }

    @Test
    public void applicableConstructorComplex() throws NoSuchMethodException {
        assertThat("locates ArrayList(Collection) constructor for Collection argument",
                Classes.getApplicableConstructor(ArrayList.class, Collection.class),
                is(ArrayList.class.getDeclaredConstructor(Collection.class)));
        assertThat("locates ArrayList(Collection) constructor for List argument",
                Classes.getApplicableConstructor(ArrayList.class, List.class),
                is(ArrayList.class.getDeclaredConstructor(Collection.class)));
        assertThat("locates ArrayList(Collection) constructor for ArrayList argument",
                Classes.getApplicableConstructor(ArrayList.class, ArrayList.class),
                is(ArrayList.class.getDeclaredConstructor(Collection.class)));
    }

    @Test
    public void applicableConstructorProtected() throws NoSuchMethodException {
        assertThat("locates protected Charset(String, String[]) constructor",
                Classes.getApplicableConstructor(Charset.class, String.class, String[].class),
                is(Charset.class.getDeclaredConstructor(String.class, String[].class)));
    }

    @Test
    public void applicableConstructorPrimitives() throws NoSuchMethodException {
        assertThat("locates primitive arg constructor for Duration",
                Classes.getApplicableConstructor(Duration.class, long.class, TimeUnit.class),
                is(Duration.class.getDeclaredConstructor(long.class, TimeUnit.class)));
        assertThat("locates primitive arg constructor for Duration with boxed args",
                Classes.getApplicableConstructor(Duration.class, Long.class, TimeUnit.class),
                is(Duration.class.getDeclaredConstructor(long.class, TimeUnit.class)));
        assertThat("locates primitive arg constructor for Duration with assignable primitive args",
                Classes.getApplicableConstructor(Duration.class, int.class, TimeUnit.class),
                is(Duration.class.getDeclaredConstructor(long.class, TimeUnit.class)));
        assertThat("locates primitive arg constructor for Duration with assignable boxed args",
                Classes.getApplicableConstructor(Duration.class, Integer.class, TimeUnit.class),
                is(Duration.class.getDeclaredConstructor(long.class, TimeUnit.class)));
    }

    @Test
    public void applicableConstructorNulls() throws NoSuchMethodException {
        assertThat("locates constructor with null args",
                Classes.getApplicableConstructor(Duration.class, long.class, Classes.Null.class),
                is(Duration.class.getDeclaredConstructor(long.class, TimeUnit.class)));
    }

    @Test(expected = NoSuchMethodException.class)
    public void applicableConstructorPrimitivesNotAssignable() throws NoSuchMethodException {
        Classes.getApplicableConstructor(Duration.class, Float.class, TimeUnit.class);
    }

    @Test(expected = NoSuchMethodException.class)
    public void applicableConstructorNotExist() throws NoSuchMethodException {
        Classes.getApplicableConstructor(String.class, long.class, int.class);
    }

    @Test
    public void applicableMethods() throws NoSuchMethodException {
        assertThat("locates String#length for no args",
                Classes.getApplicableMethod(String.class, "length"),
                is(String.class.getDeclaredMethod("length")));
        assertThat("locates only overload of String#concat with one arg",
                Classes.getApplicableMethod(String.class, "concat", String.class),
                is(String.class.getDeclaredMethod("concat", String.class)));
        assertThat("locates only overload of String#codePointCount with two args",
                Classes.getApplicableMethod(String.class, "codePointCount", int.class, int.class),
                is(String.class.getDeclaredMethod("codePointCount", int.class, int.class)));
        assertThat("locates correct overload of String#format with two args",
                Classes.getApplicableMethod(String.class, "format", String.class, Object[].class),
                is(String.class.getDeclaredMethod("format", String.class, Object[].class)));
    }

    @Test(expected = NoSuchMethodException.class)
    public void applicableMethodsNotExist() throws NoSuchMethodException {
        Classes.getApplicableMethod(String.class, "nolliston");
    }

    @Test(expected = NoSuchMethodException.class)
    public void applicableMethodsNotExistOverload() throws NoSuchMethodException {
        Classes.getApplicableMethod(String.class, "length", String.class);
    }

    @Test
    public void isAssignableFrom() {
        assertThat("String assigns to String",
                Classes.isAssignableFrom(String.class, String.class),
                is(true));
        assertThat("String assigns to Serializable",
                Classes.isAssignableFrom(Serializable.class, String.class),
                is(true));
        assertThat("String doesn't assign to Integer",
                Classes.isAssignableFrom(Integer.class, String.class),
                is(false));
        assertThat("null assigns to String",
                Classes.isAssignableFrom(String.class, Classes.Null.class),
                is(true));
    }
}
