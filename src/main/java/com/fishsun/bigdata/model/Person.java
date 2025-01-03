package com.fishsun.bigdata.model;

public class Person {
    private Integer id;
    private String name;
    private Integer age;
    private String city;

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Person)) {
            return false;
        }
        Person person = (Person) obj;
        return person.getId().equals(id)
                && person.getName().equals(name)
                && person.getAge().equals(age)
                && person.getCity().equals(city);
    }

    public Person() {
    }

    public Person(Integer id, String name, Integer age, String city) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.city = city;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
