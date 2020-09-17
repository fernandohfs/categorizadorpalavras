package com.facef.kafka.categorizadorpalavras;

import com.facef.kafka.categorizadorpalavras.utils.WordCount;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

@SpringBootApplication
public class CategorizadorpalavrasApplication {

	public static void main(String[] args) {
		SpringApplication.run(CategorizadorpalavrasApplication.class, args);
	}

	@Bean
	@SuppressWarnings("unchecked")
	public Function<KStream<Object, String>, KStream<Object, WordCount>[]> process() {

		Predicate<Object, WordCount> isSmall = (k, v) -> v.getKey().length() < 1;
		Predicate<Object, WordCount> isMedium = (k, v) -> v.getKey().length() < 1;
		Predicate<Object, WordCount> isLarge = (k, v) -> v.getKey().length() < 1;

		return input -> input.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
				.map((key, value) -> new KeyValue<>(value, value))
				.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
				.windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
				.count(Materialized.as("WordCounts-1"))
				.toStream()
				.map((key, value) -> new KeyValue<>(null, WordCount.builder()
						.key(key.key())
						.count(value)
						.start(new Date(key.window().start()))
						.end(new Date(key.window().end()))
						.build()))
				.branch(isSmall, isMedium, isLarge);
	}

}
