import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
import joblib

# 1. Load your Type 3 data
# Assuming you saved the text as 'fraud_data.csv'
# If your file is large, perfect. If it's small, we might need to generate more later.
df = pd.read_csv("cc_fraud.csv")

# 2. The "Resume-Grade" Cleanup
# We DROP 'velocity_last_24h' because we want to engineer that in Real-Time using Redis later.
# If we leave it in, the model relies on a feature we haven't built in the pipeline yet.
X = df.drop(columns=['transaction_id', 'is_fraud', 'velocity_last_24h'])
y = df['is_fraud']

# Handle categorical data (Merchant Category) via One-Hot Encoding
X = pd.get_dummies(X, columns=['merchant_category'])

# 3. Split Data
# Stratify ensures we have fraud cases in both train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

# 4. Train Model
# We use Random Forest because it's robust and easy to explain later
print("Training Model...")
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# 5. Evaluate
# We look at Recall (Catching fraud) vs Precision (Not annoying users)
y_pred = model.predict(X_test)
print("\nModel Performance:")
print(classification_report(y_test, y_pred))

# 6. Save the artifacts
# We need to save the model AND the column names (so our API knows the order)
joblib.dump(model, "fraud_model.joblib")
joblib.dump(X_train.columns.tolist(), "model_columns.joblib")

print("\nModel and Columns saved! Phase 1 Complete.")