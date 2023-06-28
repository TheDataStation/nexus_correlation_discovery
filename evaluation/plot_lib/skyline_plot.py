import json
import matplotlib.pyplot as plt

if __name__ == "__main__":
    path = "/Users/yuegong/Documents/spatio_temporal_alignment/evaluation/graph_result/chicago/skyline.json"
    with open(path) as f:
        data = json.load(f)

    num1_values = []
    num2_values = []
    for key in data.keys():
        num1, num2 = eval(key)  # Safely evaluate the string "(num1, num2)"
        num1_values.append(num1)
        num2_values.append(num2)

    # Plot the data
    plt.step(num1_values, num2_values, where="post")
    plt.scatter(num1_values, num2_values, color="red", label="Solutions")
    plt.xlabel("coverage ratio")
    plt.ylabel("clustering score")
    plt.title("Skyline Plot")
    plt.grid(True)
    plt.show()
