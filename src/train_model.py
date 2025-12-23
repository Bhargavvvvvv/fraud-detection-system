import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
import joblib

df = pd.read_csv("cc_fraud.csv")
X = df.drop(columns=['transaction_id', 'is_fraud', 'velocity_last_24h'])
y = df['is_fraud']

X = pd.get_dummies(X, columns=['merchant_category'])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred))
joblib.dump(model, "fraud_model.joblib")
joblib.dump(X_train.columns.tolist(), "model_columns.joblib")
