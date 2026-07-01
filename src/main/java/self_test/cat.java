package self_test;

public class cat {
    int age;
    String name;

    public cat() {
    }

    public cat(int age, String name) {
        this.age = age;
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "cat{" +
                "age=" + age +
                ", name='" + name + '\'' +
                '}';
    }
}
