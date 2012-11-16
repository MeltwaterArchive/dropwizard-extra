package com.datasift.dropwizard.zookeeper.util;

import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class ZNodeTest {

    @Test
    public void validPath() {
        assertThat("valid paths are valid", new ZNode("/valid/path"), is(ZNode.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPathNoRootSlash() {
        new ZNode("invalid/path");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPathNoTrailingSlash() {
        new ZNode("/invalid/path/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPathNoRelative() {
        new ZNode("/invalid/./path");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPathNoRelativeParentSlash() {
        new ZNode("/invalid/../path");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPathNull() {
        new ZNode(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPathEmpty() {
        new ZNode("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPathEmptyNode() {
        new ZNode("/invalid//path");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPathNulChar() {
        new ZNode("/invalid\u0000/path");
    }
}
