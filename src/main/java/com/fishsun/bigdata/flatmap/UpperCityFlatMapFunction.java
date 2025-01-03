package com.fishsun.bigdata.flatmap;

import com.fishsun.bigdata.model.Person;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class UpperCityFlatMapFunction extends RichFlatMapFunction<Person, Person> {
    // private ValueStateDescriptor<Person> descriptor;
    private ValueState<Person> prevPerson;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Person> descriptor = new ValueStateDescriptor<>("previous-person", Person.class);
        prevPerson = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Person person, Collector<Person> collector) throws Exception {
        prevPerson.update(person);
        person.setCity(person.getCity().toUpperCase());
        collector.collect(person);
    }
}
