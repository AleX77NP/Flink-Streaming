package galiglobal.flink;

public class Pokemon {
    private String name;
    private int height;
    private int weight;
    private int baseExperience;

    @Override
    public String toString() {
        return "Pokemon: " + name;
    }

    public Pokemon(String name, int height, int weight, int baseExperience, boolean isDefault) {
        this.name = name;
        this.height = height;
        this.weight = weight;
        this.baseExperience = baseExperience;
        this.isDefault = isDefault;
    }

    private boolean isDefault;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getBaseExperience() {
        return baseExperience;
    }

    public void setBaseExperience(int baseExperience) {
        this.baseExperience = baseExperience;
    }

    public boolean isDefault() {
        return isDefault;
    }

    public void setDefault(boolean aDefault) {
        isDefault = aDefault;
    }
}
