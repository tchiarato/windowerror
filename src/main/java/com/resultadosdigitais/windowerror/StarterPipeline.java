package com.resultadosdigitais.windowerror;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(
            PipelineOptionsFactory.fromArgs(args).withValidation().create());

        pipeline
            .apply("Mock", new DummyDataBuilder())
            .apply("Windowing",
//                Window.<KV<String, String>>into(Sessions.withGapDuration(Duration.standardMinutes(2)))
                Window.<KV<String, String>>into(FixedWindows.of(Duration.standardMinutes(2)))
                    .triggering(AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings( AfterProcessingTime
                            .pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30))))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply("GroupByKey", GroupByKey.create())
            .apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    LOG.info(String.valueOf(c.element()));
                }
            }));
        ;


        pipeline.run();
    }

    static class DummyDataBuilder extends PTransform<PBegin, PCollection<KV<String, String>>> {

        private Instant baseTime = new Instant(0L);

        @Override
        public PCollection<KV<String, String>> expand(PBegin input) {
            return input.apply("Mocking", mock());
        }

        private TestStream<KV<String, String>> mock() {
            return TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .addElements(TimestampedValue.of(KV.of("table_a", "value_1"), baseTime.plus(Duration.standardSeconds(1))))
                .addElements(TimestampedValue.of(KV.of("table_a", "value_2"), baseTime.plus(Duration.standardSeconds(2))))

                .advanceProcessingTime(Duration.standardSeconds(31))

                .addElements(TimestampedValue.of(KV.of("table_a", "value_3"), baseTime.plus(Duration.standardSeconds(33))))
                .addElements(TimestampedValue.of(KV.of("table_a", "value_4"), baseTime.plus(Duration.standardSeconds(34))))

                .advanceProcessingTime(Duration.standardSeconds(61))

                .addElements(TimestampedValue.of(KV.of("table_a", "value_5"), baseTime.plus(Duration.standardSeconds(65))))
                .addElements(TimestampedValue.of(KV.of("table_a", "value_6"), baseTime.plus(Duration.standardSeconds(66))))

                .advanceProcessingTime(Duration.standardSeconds(91))

                .addElements(TimestampedValue.of(KV.of("table_a", "value_7"), baseTime.plus(Duration.standardSeconds(97))))
                .addElements(TimestampedValue.of(KV.of("table_a", "value_8"), baseTime.plus(Duration.standardSeconds(98))))

                .advanceProcessingTime(Duration.standardSeconds(121))
                .advanceWatermarkTo(baseTime.plus(Duration.standardSeconds(121)))

                .addElements(TimestampedValue.of(KV.of("table_a", "value_9"), baseTime.plus(Duration.standardSeconds(129))))
                .addElements(TimestampedValue.of(KV.of("table_a", "value_10"), baseTime.plus(Duration.standardSeconds(130))))
                .advanceWatermarkToInfinity();
        }
    }
}
