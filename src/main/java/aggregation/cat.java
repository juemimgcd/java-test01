package aggregation;

public class cat {
    int age;
    String name;
    long weight;

    public cat() {
    }

    public cat(int age, String name, long weight) {
        this.age = age;
        this.name = name;
        this.weight = weight;
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

    public long getWeight() {
        return weight;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "cat{" +
                "age=" + age +
                ", name='" + name + '\'' +
                ", weight=" + weight +
                '}';
    }
}
