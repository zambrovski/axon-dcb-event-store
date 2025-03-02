package io.event.thinking.eventstore.inmemory;

import io.event.thinking.eventstore.api.Criteria;
import io.event.thinking.eventstore.api.Criterion;
import io.event.thinking.eventstore.api.InvalidConsistencyConditionException;
import io.event.thinking.eventstore.api.MarkedEvents;
import io.event.thinking.eventstore.api.SequencedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.stream.IntStream;

import static io.event.thinking.eventstore.api.ConsistencyCondition.consistencyCondition;
import static io.event.thinking.eventstore.api.Criteria.anyOf;
import static io.event.thinking.eventstore.api.Criterion.allOf;
import static io.event.thinking.eventstore.api.SequencedEvent.sequencedEvent;
import static io.event.thinking.eventstore.api.Index.index;
import static io.event.thinking.eventstore.api.Event.event;
import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryEventStoreTest {

    private InMemoryEventStore eventStore;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
    }

    @Test
    void appendWithNoCondition() {
        StepVerifier.create(eventStore.append(event(emptyPayload())))
                    .expectNext(0L)
                    .verifyComplete();
    }

    @Test
    void appendTransaction() {
        StepVerifier.create(eventStore.append(List.of(event(emptyPayload()),
                                                      event(emptyPayload()),
                                                      event(emptyPayload()))))
                    .expectNext(2L)
                    .verifyComplete();
    }

    @Test
    void multipleAppendsWithNoCondition() {
        var event = event(emptyPayload());
        var appends = Flux.merge(eventStore.append(event),
                                 eventStore.append(event),
                                 eventStore.append(event),
                                 eventStore.append(event),
                                 eventStore.append(event));
        StepVerifier.create(appends)
                    .expectNext(0L, 1L, 2L, 3L, 4L)
                    .verifyComplete();
    }

    @Test
    void multipleAppendsWithNonConflictingCondition() {
        var index1 = index("key1", "value1");
        var index2 = index("key2", "value2");
        var event1 = event(emptyPayload(), index1);
        var event2 = event(emptyPayload(), index2);
        var consistencyCondition1 = consistencyCondition(-1L, Criteria.anyOf(Criterion.allOf(index1)));
        var consistencyCondition2 = consistencyCondition(0L, Criteria.anyOf(Criterion.allOf(index2)));

        var appends = Flux.merge(eventStore.append(event1, consistencyCondition1),
                                 eventStore.append(event2, consistencyCondition2));
        StepVerifier.create(appends)
                    .expectNext(0L, 1L)
                    .verifyComplete();
    }

    @Test
    void multipleAppendsWithConflictingIndicessAndCorrectConsistencyMarker() {
        var index = index("key", "value");
        var event1 = event(emptyPayload(), index);
        var event2 = event(emptyPayload(), index);
        var consistencyCondition1 = consistencyCondition(0L, Criteria.anyOf(Criterion.allOf(index)));
        var consistencyCondition2 = consistencyCondition(1L, Criteria.anyOf(Criterion.allOf(index)));

        var appends = Flux.merge(eventStore.append(event1, consistencyCondition1),
                                 eventStore.append(event2, consistencyCondition2));
        StepVerifier.create(appends)
                    .expectNext(0L, 1L)
                    .verifyComplete();
    }

    @Test
    void multipleAppendsWithConflictingCondition() {
        var index = index("key", "value");
        var event = event(emptyPayload(), index);
        var consistencyCondition = consistencyCondition(-1L, Criteria.anyOf(Criterion.allOf(index)));

        var appends = Flux.merge(eventStore.append(event, consistencyCondition),
                                 eventStore.append(event, consistencyCondition));
        StepVerifier.create(appends)
                    .expectNext(0L)
                    .verifyError(InvalidConsistencyConditionException.class);
    }

    @Test
    void appendWithNonExistingConsistencyMarker() {
        StepVerifier.create(eventStore.append(event(emptyPayload()), consistencyCondition(100L, Criteria.anyOf())))
                    .expectNext(0L)
                    .verifyComplete();
    }

    @Test
    void noGapsAppend() throws InterruptedException {
        Thread[] appenders = new Thread[12];
        int appendsPerAppender = 1_000;

        for (int i = 0; i < appenders.length; i++) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < appendsPerAppender; j++) {
                    eventStore.append(event(emptyPayload()))
                              .block();
                }
            });
            appenders[i] = t;
            t.start();
        }
        for (Thread thread : appenders) {
            thread.join();
        }

        int expectedSize = appenders.length * appendsPerAppender;
        StepVerifier.create(eventStore.read()
                                      .flux()
                                      .map(SequencedEvent::sequence))
                    .expectNextSequence(() -> IntStream.range(0, expectedSize)
                                                       .asLongStream()
                                                       .iterator())
                    .verifyComplete();
    }

    @Test
    void readFromEmptyEventStore() {
        var source = eventStore.read(Criteria.anyOf(Criterion.allOf(index("key", "value"))));

        assertEquals(0L, source.consistencyMarker());
        StepVerifier.create(source.flux())
                    .verifyComplete();
    }

    @Test
    void read() {
        var index1 = index("key1", "value1");
        var index2 = index("key2", "value2");
        var event1 = event(payload("event1"), index1);
        var event2 = event(payload("event2"), index2);

        var appends = Flux.merge(eventStore.append(event1),
                                 eventStore.append(event2),
                                 eventStore.append(event1),
                                 eventStore.append(event1),
                                 eventStore.append(event2));
        StepVerifier.create(appends)
                    .expectNext(0L, 1L, 2L, 3L, 4L)
                    .verifyComplete();

        var source1 = eventStore.read(Criteria.anyOf(Criterion.allOf(index1)));
        assertEquals(5L, source1.consistencyMarker());
        StepVerifier.create(source1.flux())
                    .expectNext(sequencedEvent(0L, event1),
                                sequencedEvent(2L, event1),
                                sequencedEvent(3L, event1))
                    .verifyComplete();

        StepVerifier.create(eventStore.append(event2))
                    .expectNext(5L)
                    .verifyComplete();
        var source2 = eventStore.read(Criteria.anyOf(Criterion.allOf(index2)));
        assertEquals(6L, source2.consistencyMarker());
        StepVerifier.create(source2.flux())
                    .expectNext(sequencedEvent(1L, event2),
                                sequencedEvent(4L, event2),
                                sequencedEvent(5L, event2))
                    .verifyComplete();
    }

    @Test
    void readFromEmptyEventStoreWithoutCriteria() {
        StepVerifier.create(eventStore.read()
                                      .flux())
                    .verifyComplete();
    }

    @Test
    void readWithNullCriteria() {
        var index = index("key", "value");
        var event = event(payload("event"), index);
        eventStore.append(event)
                  .block();

        MarkedEvents result = eventStore.read(0L, null);
        assertEquals(1L, result.consistencyMarker());
        StepVerifier.create(result.flux())
                    .expectNext(sequencedEvent(0L, event))
                    .verifyComplete();
    }

    @Test
    void readAllEvents() {
        var index = index("key", "value");
        var event1 = event(payload("event1"), index);
        var event2 = event(payload("event2"), index);
        var event3 = event(payload("event3"), index);
        var appends = Flux.merge(eventStore.append(event1),
                                 eventStore.append(event2),
                                 eventStore.append(event3));
        StepVerifier.create(appends)
                    .expectNext(0L, 1L, 2L)
                    .verifyComplete();

        StepVerifier.create(eventStore.read()
                                      .flux())
                    .expectNext(sequencedEvent(0L, event1),
                                sequencedEvent(1L, event2),
                                sequencedEvent(2L, event3))
                    .verifyComplete();
    }

    @Test
    void readFromSeq() {
        var index = index("key", "value");
        var event1 = event(payload("event1"), index);
        var event2 = event(payload("event2"), index);
        var event3 = event(payload("event3"), index);
        var appends = Flux.merge(eventStore.append(event1),
                                 eventStore.append(event2),
                                 eventStore.append(event3));
        StepVerifier.create(appends)
                    .expectNext(0L, 1L, 2L)
                    .verifyComplete();

        StepVerifier.create(eventStore.read(1L)
                                      .flux())
                    .expectNext(sequencedEvent(1L, event2),
                                sequencedEvent(2L, event3))
                    .verifyComplete();
    }

    @Test
    void readFromInvalidSeq() {
        var index = index("key", "value");
        var event1 = event(payload("event1"), index);
        var event2 = event(payload("event2"), index);
        var event3 = event(payload("event3"), index);
        var appends = Flux.merge(eventStore.append(event1),
                                 eventStore.append(event2),
                                 eventStore.append(event3));
        StepVerifier.create(appends)
                    .expectNext(0L, 1L, 2L)
                    .verifyComplete();

        StepVerifier.create(eventStore.read(5L)
                                      .flux())
                    .verifyComplete();
    }

    private static byte[] emptyPayload() {
        return new byte[]{};
    }

    private static byte[] payload(String payload) {
        return payload.getBytes();
    }
}
