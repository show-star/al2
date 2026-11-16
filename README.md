# Big Data Practical 2: Data Lakehouse with Delta Lake

**Name:** Arvind Yadav
**Roll no:** 31011123065

## Aim

Data Lakehouse: Create a Delta Lake table and demonstrate "Time Travel" and "ACID Transactions" on Spark.

## Theory

**Data lakehouse.** A data lake stores large amounts of raw files cheaply but gives no guarantees about the data. A data warehouse gives reliable tables but is costly and less flexible. A data lakehouse combines both: the data stays in files, and a table layer on top adds reliability and fast queries.

**Delta Lake.** Delta Lake is an open-source table layer for Spark. A Delta table is a folder of Parquet data files plus a `_delta_log` folder. Every change to the table (insert, update, delete, overwrite) is recorded in `_delta_log` as a new numbered version: 0, 1, 2 and so on.

**ACID transactions.** Because every change is one entry in the log, each change is a transaction:

| Property | Meaning in Delta Lake |
| --- | --- |
| Atomicity | A write happens completely or not at all; a failed job leaves no half-written data |
| Consistency | Data that does not match the table's schema is rejected |
| Isolation | Readers see a complete version of the table even while another job is writing |
| Durability | A committed version is saved on disk and is not lost |

**Time travel.** Old versions stay in the log, so an older version of the table can be read again by its version number or timestamp. This is used to check old data, compare changes and undo mistakes.

**How these are done in PySpark.**

```python
# Create a Delta table (version 0)
df.write.format("delta").mode("overwrite").save("student_table")

# Time travel: read an older version
spark.read.format("delta").option("versionAsOf", 0).load("student_table").show()

# See all versions (transactions) of the table
from delta.tables import DeltaTable
DeltaTable.forPath(spark, "student_table").history().show()
```

## Files

| File | Purpose |
| --- | --- |
| `delta_demo.py` | Starts Spark with Delta Lake enabled, creates the student data and displays it |
| `requirements.txt` | Python packages needed (PySpark and delta-spark) |

## Requirements (Windows)

- Windows 10 or 11
- Python 3.10 or above (tick **Add python.exe to PATH** during installation)
- Java 17 (JDK), with `JAVA_HOME` set
- PySpark 4.2.0 and delta-spark 4.4.0 (installed in step 3)
- Internet connection for the first run (Spark downloads the Delta Lake JAR files)

## Steps to run (Windows PowerShell)

**1. Check Python and Java**

```powershell
python --version
java -version
```

If `java` is not found, install JDK 17 (for example Eclipse Temurin 17 from <https://adoptium.net>) and choose **Set JAVA_HOME variable** in the installer. Close PowerShell and open it again after installing.

**2. Download the code**

```powershell
git clone https://github.com/show-star/al2.git
cd al2
```

Without Git: on the GitHub page click **Code > Download ZIP**, extract it, then open PowerShell inside the extracted folder.

**3. Create a virtual environment and install the packages**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If the second command gives a "running scripts is disabled" error, run this once and then repeat it:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

**4. Check the installed versions**

```powershell
pip show pyspark
pip show delta-spark
```

PySpark should show version 4.2.0 and delta-spark version 4.4.0.

**5. Run the program**

```powershell
python delta_demo.py
```

The first run takes longer because Spark downloads the Delta Lake JAR files from Maven Central. A long list of `org.apache...` lines is printed while this happens.

## Code explanation

1. **Imports.** `SparkSession` starts Spark. `configure_spark_with_delta_pip` adds the Delta Lake JAR files that match the installed `delta-spark` package.
2. **Delta configuration.** `spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension` adds Delta commands to Spark SQL. `spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog` lets Spark's catalog manage Delta tables.
3. **Spark session.** `local[*]` runs Spark on this computer using all CPU cores. `getOrCreate()` starts the session.
4. **Sample data.** A Python list of six students, each with `id`, `name` and `marks`.
5. **DataFrame.** `spark.createDataFrame(data, ["id", "name", "marks"])` turns the list into a Spark DataFrame with three columns.
6. **Display.** `df.show()` prints the DataFrame as a table.
7. **Stop.** `spark.stop()` closes the Spark session.

## Output

```text
Spark started successfully!
Student Data:
+---+--------+-----+
| id|    name|marks|
+---+--------+-----+
|  1|Siddhant|   90|
|  2|  Shreya|   88|
|  3|  Shruti|   75|
|  4| Chinmay|   90|
|  5|  Vishnu|   88|
|  6|  Nikita|   75|
+---+--------+-----+
```

On Windows, lines such as `:: retrieving :: org.apache.spark#spark-submit-parent`, `Exception while deleting Spark temp dir` and `SUCCESS: The process with PID ... has been terminated` may also appear. They are normal and can be ignored.

## Conclusion

Spark was started with the Delta Lake extension and catalog, and the student data was created as a DataFrame and displayed. Delta Lake stores such data as a table with a transaction log, which gives ACID transactions and allows time travel to older versions of the table.
