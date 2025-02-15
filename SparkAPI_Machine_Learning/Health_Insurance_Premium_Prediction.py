# Databricks notebook source
# MAGIC %md
# MAGIC | Version | Date       | Developer | Remark             |
# MAGIC |---------|------------|-----------|--------------------|
# MAGIC | 1.0     | Feb-1-2025 | Johnson | Initial version: build models with sklearn & MLlib    |

# COMMAND ----------

# MAGIC %md
# MAGIC **Context**
# MAGIC
# MAGIC The insurance.csv dataset contains 1338 observations (rows) and 7 features (columns). The dataset contains 4 numerical features (age, bmi, children and expenses) and 3 nominal features (sex, smoker and region) that were converted into factors with numerical value designated for each level.

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Exploratory Data Analysis

# COMMAND ----------

# MAGIC %run ./Common_Function

# COMMAND ----------

data = '/Workspace/Users/hejiekun@hotmail.com/insurance.csv'
df = pd.read_csv(data)
df_sp = spark.createDataFrame(df)
display(df_sp.limit(10))

# COMMAND ----------

num_rows = df_sp.count()  # Count the number of rows
num_cols = len(df_sp.columns)  # Count the number of columns
print(f'{num_rows} Records,{num_cols} Features.')

# COMMAND ----------

df_sp.printSchema()

# COMMAND ----------

df_pd = df_sp.toPandas() # Transfer to Pandas 

print(df.isnull().sum())

# COMMAND ----------

sns.set(style="whitegrid")

fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# Distribution of 'age'
sns.histplot(df_pd['age'], kde=True, ax=axes[0, 0], color="skyblue")
axes[0, 0].set_title('Distribution of Age')
# Distribution of 'bmi'
sns.histplot(df_pd['bmi'], kde=True, ax=axes[0, 1], color="salmon")
axes[0, 1].set_title('Distribution of BMI')
# Distribution of 'children'
sns.countplot(x='children', data=df_pd, palette="viridis", ax=axes[1, 0])
axes[1, 0].set_title('Count of Children')
# Distribution of 'expenses'
sns.histplot(df['expenses'], kde=True, ax=axes[1, 1], color="green")
axes[1, 1].set_title('Distribution of Expenses')

plt.tight_layout()
plt.show()

# Pairplot to see relationships and scatter patterns
# sns.pairplot(df, hue="smoker", palette="coolwarm")
# plt.show()

# COMMAND ----------

# Using pd.get_dummies to encoding the categorical variable
df_encoded = pd.get_dummies(df, drop_first=True)

# Calculate the correlation between encoded features
corr = df_encoded.corr()

# Focus on correlation with target 'expenses'
charges_corr = corr['expenses'].sort_values(ascending=False)

# Show correlation results with charges
print(charges_corr)

# Visualization of correlation in heatmap form
plt.figure(figsize=(8, 6))
sns.heatmap(corr[['expenses']], annot=True, cmap='coolwarm', vmin=-1, vmax=1)
plt.title("Correlation Heatmap with 'expenses'")
plt.show()



# COMMAND ----------

# Check the missing values
print(df.isnull().sum())

# Impute missing values ​​(if any) for numeric columns with median
# df = df_encoded.fillna(df_encoded.median())

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Data Engineering

# COMMAND ----------

# DBTITLE 1,pipeline
pipeline_model = pipeline.fit(df_sp)  # Fit the pipeline
df_transformed = pipeline_model.transform(df_sp)  # Transform the data

# Show transformed data
display(df_transformed.select("sex", "sex_index", "smoker", "smoker_index", "region", "region_encoded", "bmi_normalized", "age_normalized").limit(5))

# COMMAND ----------

# DBTITLE 1,vectorize
# Call the function with df_transformed as input
df_final = vectorize_features(df_transformed)
display(df_final.limit(5))

# COMMAND ----------

# Make sure we do pd.get_dummies() for the entire dataset
df_encoded = pd.get_dummies(df, drop_first=True)

# Separate features and targets
X = df_encoded.drop('expenses', axis=1)  # Fitur
y = df_encoded['expenses']  # Target (charges)

# Check some data to make sure there are no remaining strings
print(X.head())

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

# Features standarization
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# COMMAND ----------

# Check the data type of each column
print(X.dtypes)

# COMMAND ----------

# Split data into 80% training and 20% testing
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

# Check the dimensions of training and testing data
print(X_train.shape, X_test.shape)

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

# Initialize scaler
scaler = StandardScaler()

# Standardization of training and testing data
X_train_scaled = scaler.fit_transform(X_train)  # Fit pada data training
X_test_scaled = scaler.transform(X_test)  # Transform data testing

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Model Training and Evaluation Metrics

# COMMAND ----------

# DBTITLE 1,sklearn
from sklearn.ensemble import RandomForestRegressor

rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(X_train_scaled, y_train)
predictions_rf = rf.predict(X_test_scaled)

from sklearn.metrics import mean_squared_error, r2_score

mse_linear = mean_squared_error(y_test, predictions_rf)
r2_linear = r2_score(y_test, predictions_rf)

print(f"Linear Regression - MSE: {mse_linear}, R2: {r2_linear}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.Explain The Model 

# COMMAND ----------

import shap

from sklearn.ensemble import RandomForestRegressor

rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(X_train_scaled, y_train)
predictions_rf = rf.predict(X_test_scaled)

explainer = shap.Explainer(rf, X_train_scaled)
shap_values = explainer(X_test_scaled, check_additivity=False)



feature_names = ['age', 'sex', 'bmi', 'children', 'smoker', 'region_southeast','region_northwest','region_northeast']
shap.summary_plot(shap_values, feature_names=feature_names)
