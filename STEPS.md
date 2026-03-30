1. Open VS Code
2. Create a new Jupyter Notebook
3. Enter the following code snippet into the first notebook cell
```
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("PySparkSampleExample").getOrCreate()

# Create a sample DataFrame
data = [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E"), (6, "F"), (7, "G"), (8, "H"), (9, "I"), (10, "J")]
columns = ["id", "value"]
df = spark.createDataFrame(data, columns)

# Sample the DataFrame (approx. 50% of rows)
# withReplacement=False (default), fraction=0.5
sampled_df = df.sample(False, 0.5)

print("Original DataFrame count:", df.count())
print("Sampled DataFrame count:", sampled_df.count())
print("Sampled rows:")
sampled_df.show()
```
4. Click the "Select Kernel" button in the top right of the notebook
5. Click "Select Another Kernel"
6. Click "Remote Spark Kernel"
7. Select the first one in the list (it might be "andm-jupyter"). This might take a really long time.
8. Execute all the cells in the notebook