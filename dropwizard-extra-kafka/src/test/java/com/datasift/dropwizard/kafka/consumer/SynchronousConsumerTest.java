package com.datasift.dropwizard.kafka.consumer;

import com.datasift.dropwizard.kafka.KafkaConsumerFactory;
import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.util.Duration;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.validation.Validation;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AbstractLifeCycle.class)
@PowerMockIgnore("javax.management.*")
public class SynchronousConsumerTest {

    private static final Decoder<byte[]> DefaultDecoder = new DefaultDecoder(new VerifiableProperties());

    private KafkaConsumerFactory configuration = null;

    @Before
    public void setup() throws Exception {
        configuration = new ConfigurationFactory<>(
                KafkaConsumerFactory.class,
                Validation.buildDefaultValidatorFactory().getValidator(),
                Jackson.newObjectMapper(),
                "dw").build(new File(Resources.getResource("yaml/consumer.yaml").toURI()));
    }

    @Test
    public void testIsRunningAfterUnrecoverableException() throws Exception {

        //Mock a KafkaStream
        KafkaStream messageStream = Mockito.mock(KafkaStream.class);
        Map<String, List<KafkaStream>> mockedMessageStreams = Collections.singletonMap("TesTopic",Collections.singletonList(messageStream));
        ConsumerConnector consumerConnector = Mockito.mock(ConsumerConnector.class);
        when(consumerConnector.createMessageStreams(Mockito.anyMap(),Mockito.any(Decoder.class),Mockito.any(Decoder.class))).thenReturn(mockedMessageStreams);
        Server jettyServer = PowerMockito.mock(Server.class);
        Mockito.doNothing().when(jettyServer).stop();
        //Mock an Executor to simply run the StreamProcessorRunnable
        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        when(executor.isTerminated()).thenReturn(false);
        when(executor.isShutdown()).thenReturn(false);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).execute(Mockito.any(Runnable.class));
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        //Mock the StreamProcessor - Throws Unrecoverable IllegalStateException
        StreamProcessor processor = Mockito.mock(StreamProcessor.class);
        Mockito.doThrow(new IllegalStateException()).when(processor).process(Mockito.any(Iterable.class), Mockito.anyString());
        final SynchronousConsumer consumer = new SynchronousConsumer(
                consumerConnector,
                configuration.getPartitions(),
                DefaultDecoder,
                DefaultDecoder,
                processor,
                executor,
                configuration.getInitialRecoveryDelay(),
                configuration.getMaxRecoveryDelay(),
                configuration.getRetryResetDelay(),
                configuration.getMaxRecoveryAttempts(),
                configuration.isShutdownOnFatal(),
                Duration.seconds(0));
        consumer.serverStarted(jettyServer);
        assertTrue(consumer.isRunning());
        consumer.start();
        assertFalse(consumer.isRunning());
        Mockito.verify(jettyServer, timeout(1000).times(0)).stop();
    }

    @Test
    public void testIsRunningAfterRecoverableException() throws Exception {

        //Mock a KafkaStream
        KafkaStream messageStream = Mockito.mock(KafkaStream.class);
        Map<String, List<KafkaStream>> mockedMessageStreams = Collections.singletonMap("TesTopic",Collections.singletonList(messageStream));
        ConsumerConnector consumerConnector = Mockito.mock(ConsumerConnector.class);
        when(consumerConnector.createMessageStreams(Mockito.anyMap(),Mockito.any(Decoder.class),Mockito.any(Decoder.class))).thenReturn(mockedMessageStreams);
        Server jettyServer = PowerMockito.mock(Server.class);
        Mockito.doNothing().when(jettyServer).stop();
        //Mock an Executor to simply run the StreamProcessorRunnable
        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        when(executor.isTerminated()).thenReturn(false);
        when(executor.isShutdown()).thenReturn(false);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).execute(Mockito.any(Runnable.class));
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        //Mock the StreamProcessor - Throws recoverable Exception and then recovers
        StreamProcessor processor = Mockito.mock(StreamProcessor.class);
        Mockito.doThrow(new RuntimeException()).doNothing().when(processor).process(Mockito.any(Iterable.class), Mockito.anyString());
        final SynchronousConsumer consumer = new SynchronousConsumer(
                consumerConnector,
                configuration.getPartitions(),
                DefaultDecoder,
                DefaultDecoder,
                processor,
                executor,
                configuration.getInitialRecoveryDelay(),
                configuration.getMaxRecoveryDelay(),
                configuration.getRetryResetDelay(),
                configuration.getMaxRecoveryAttempts(),
                configuration.isShutdownOnFatal(),
                Duration.seconds(0));
        consumer.serverStarted(jettyServer);
        assertTrue(consumer.isRunning());
        consumer.start();
        assertTrue(consumer.isRunning());
        Mockito.verify(jettyServer, timeout(1000).times(0)).stop();
    }

    @Test
    public void testIsRunningAfterMaximumRecoverableException() throws Exception {

        //Mock a KafkaStream
        KafkaStream messageStream = Mockito.mock(KafkaStream.class);
        Map<String, List<KafkaStream>> mockedMessageStreams = Collections.singletonMap("TesTopic",Collections.singletonList(messageStream));
        ConsumerConnector consumerConnector = Mockito.mock(ConsumerConnector.class);
        when(consumerConnector.createMessageStreams(Mockito.anyMap(),Mockito.any(Decoder.class),Mockito.any(Decoder.class))).thenReturn(mockedMessageStreams);
        Server jettyServer = PowerMockito.mock(Server.class);
        Mockito.doNothing().when(jettyServer).stop();
        //Mock an Executor to simply run the StreamProcessorRunnable
        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        when(executor.isTerminated()).thenReturn(false);
        when(executor.isShutdown()).thenReturn(false);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).execute(Mockito.any(Runnable.class));
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        //Mock the StreamProcessor - Throws maximum number of recoverable Exception and transition to unrecoverable
        StreamProcessor processor = Mockito.mock(StreamProcessor.class);
        final int maxRetries = 3;
        Mockito.doThrow(new RuntimeException()).
                doThrow(new RuntimeException()).
                doThrow(new RuntimeException()).when(processor).process(Mockito.any(Iterable.class), Mockito.anyString());
        final SynchronousConsumer consumer = new SynchronousConsumer(
                consumerConnector,
                configuration.getPartitions(),
                DefaultDecoder,
                DefaultDecoder,
                processor,
                executor,
                configuration.getInitialRecoveryDelay(),
                configuration.getMaxRecoveryDelay(),
                configuration.getRetryResetDelay(),
                maxRetries,
                configuration.isShutdownOnFatal(),
                Duration.seconds(0));
        consumer.serverStarted(jettyServer);
        assertTrue(consumer.isRunning());
        consumer.start();
        assertFalse(consumer.isRunning());
        Mockito.verify(jettyServer, timeout(1000).times(0)).stop();
    }

    @Test
    public void testServerShutDownOnUnrecoverableException() throws Exception {

        //Mock a KafkaStream
        KafkaStream messageStream = Mockito.mock(KafkaStream.class);
        Map<String, List<KafkaStream>> mockedMessageStreams = Collections.singletonMap("TesTopic",Collections.singletonList(messageStream));
        ConsumerConnector consumerConnector = Mockito.mock(ConsumerConnector.class);
        when(consumerConnector.createMessageStreams(Mockito.anyMap(),Mockito.any(Decoder.class),Mockito.any(Decoder.class))).thenReturn(mockedMessageStreams);
        Server jettyServer = PowerMockito.mock(Server.class);
        Mockito.doNothing().when(jettyServer).stop();
        //Mock an Executor to simply run the StreamProcessorRunnable
        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        when(executor.isTerminated()).thenReturn(false);
        when(executor.isShutdown()).thenReturn(false);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).execute(Mockito.any(Runnable.class));
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        //Mock the StreamProcessor - Throws Unrecoverable IllegalStateException
        StreamProcessor processor = Mockito.mock(StreamProcessor.class);
        final boolean shutDownServerOnUnrecoverableError = true;
        Mockito.doThrow(new IllegalStateException()).when(processor).process(Mockito.any(Iterable.class), Mockito.anyString());
        final SynchronousConsumer consumer = new SynchronousConsumer(
                consumerConnector,
                configuration.getPartitions(),
                DefaultDecoder,
                DefaultDecoder,
                processor,
                executor,
                configuration.getInitialRecoveryDelay(),
                configuration.getMaxRecoveryDelay(),
                configuration.getRetryResetDelay(),
                configuration.getMaxRecoveryAttempts(),
                shutDownServerOnUnrecoverableError,
                Duration.seconds(0));
        consumer.serverStarted(jettyServer);
        assertTrue(consumer.isRunning());
        consumer.start();
        assertFalse(consumer.isRunning());
        Mockito.verify(jettyServer, timeout(1000).times(1)).stop();
    }

    @Test
    public void testServerShutDownAfterMaximumRecoverableException() throws Exception {

        //Mock a KafkaStream
        KafkaStream messageStream = Mockito.mock(KafkaStream.class);
        Map<String, List<KafkaStream>> mockedMessageStreams = Collections.singletonMap("TesTopic",Collections.singletonList(messageStream));
        ConsumerConnector consumerConnector = Mockito.mock(ConsumerConnector.class);
        when(consumerConnector.createMessageStreams(Mockito.anyMap(),Mockito.any(Decoder.class),Mockito.any(Decoder.class))).thenReturn(mockedMessageStreams);
        Server jettyServer = PowerMockito.mock(Server.class);
        Mockito.doNothing().when(jettyServer).stop();
        //Mock an Executor to simply run the StreamProcessorRunnable
        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        when(executor.isTerminated()).thenReturn(false);
        when(executor.isShutdown()).thenReturn(false);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).execute(Mockito.any(Runnable.class));
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        //Mock the StreamProcessor - Throws Unrecoverable IllegalStateException
        StreamProcessor processor = Mockito.mock(StreamProcessor.class);
        final boolean shutDownServerOnUnrecoverableError = true;
        final int maxRetries = 3;
        Mockito.doThrow(new RuntimeException()).
                doThrow(new RuntimeException()).
                doThrow(new RuntimeException()).when(processor).process(Mockito.any(Iterable.class), Mockito.anyString());
        final SynchronousConsumer consumer = new SynchronousConsumer(
                consumerConnector,
                configuration.getPartitions(),
                DefaultDecoder,
                DefaultDecoder,
                processor,
                executor,
                configuration.getInitialRecoveryDelay(),
                configuration.getMaxRecoveryDelay(),
                configuration.getRetryResetDelay(),
                maxRetries,
                shutDownServerOnUnrecoverableError,
                Duration.seconds(0));
        consumer.serverStarted(jettyServer);
        assertTrue(consumer.isRunning());
        consumer.start();
        assertFalse(consumer.isRunning());
        Mockito.verify(jettyServer, timeout(1000).times(1)).stop();
    }



    @Test
    public void testDurationForResettingErrorHandlingState() throws Exception {

        //Mock a KafkaStream
        KafkaStream messageStream = Mockito.mock(KafkaStream.class);
        Map<String, List<KafkaStream>> mockedMessageStreams = Collections.singletonMap("TesTopic",Collections.singletonList(messageStream));
        ConsumerConnector consumerConnector = Mockito.mock(ConsumerConnector.class);
        when(consumerConnector.createMessageStreams(Mockito.anyMap(),Mockito.any(Decoder.class),Mockito.any(Decoder.class))).thenReturn(mockedMessageStreams);
        Server jettyServer = PowerMockito.mock(Server.class);
        Mockito.doNothing().when(jettyServer).stop();
        //Mock an Executor to simply run the StreamProcessorRunnable
        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        when(executor.isTerminated()).thenReturn(false);
        when(executor.isShutdown()).thenReturn(false);
        final long sleepTime = 100;
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Thread.sleep(sleepTime);
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).execute(Mockito.any(Runnable.class));
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Thread.sleep(sleepTime);
                Runnable runnable = (Runnable)args[0];
                runnable.run();
                return null;
            }
        }).when(executor).schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        //Mock the StreamProcessor - Throws Unrecoverable IllegalStateException
        StreamProcessor processor = Mockito.mock(StreamProcessor.class);
        final boolean shutDownServerOnUnrecoverableError = true;
        final int maxRetries = 3;
        final Duration durationForResettingErrorHandlingState = Duration.milliseconds(50);
        Mockito.doThrow(new RuntimeException()).
                doThrow(new RuntimeException()).
                doThrow(new RuntimeException()).
                doThrow(new RuntimeException()).
                doThrow(new RuntimeException()).
                doThrow(new RuntimeException()).
                doNothing().when(processor).process(Mockito.any(Iterable.class), Mockito.anyString());
        final SynchronousConsumer consumer = new SynchronousConsumer(
                consumerConnector,
                configuration.getPartitions(),
                DefaultDecoder,
                DefaultDecoder,
                processor,
                executor,
                configuration.getInitialRecoveryDelay(),
                configuration.getMaxRecoveryDelay(),
                durationForResettingErrorHandlingState,
                maxRetries,
                shutDownServerOnUnrecoverableError,
                Duration.seconds(0));
        consumer.serverStarted(jettyServer);
        assertTrue(consumer.isRunning());
        consumer.start();
        assertTrue(consumer.isRunning());
        Mockito.verify(jettyServer, timeout(1000).times(0)).stop();
    }


}
