import time
import json
import pandas as pd
import joblib
import streamlit as st
import altair as alt
import os
import sys

# Add the current directory to path so we can import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_kafka_consumer, get_redis_client

# --- PAGE CONFIG ---
st.set_page_config(page_title="🛡️ Fraud Guardian Live", layout="wide", page_icon="🛡️")

# --- PATH CONFIG ---
# This helps the app find the model file whether you run it from root or src
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(BASE_DIR, 'models', 'fraud_model.joblib')
COLUMNS_PATH = os.path.join(BASE_DIR, 'models', 'model_columns.joblib')

# --- INIT BACKEND ---
@st.cache_resource
def init_resources():
    # Load Model
    try:
        model = joblib.load(MODEL_PATH)
        model_cols = joblib.load(COLUMNS_PATH)
    except FileNotFoundError:
        st.error(f"Model files not found at {MODEL_PATH}. Did you run train_model.py?")
        st.stop()
    
    # Get connections from utils.py
    redis = get_redis_client()
    consumer = get_kafka_consumer('streamlit-dashboard-group')
    
    return model, model_cols, redis, consumer

model, model_columns, redis, consumer = init_resources()

# --- DASHBOARD LAYOUT ---
st.title("🛡️ Real-Time Fraud Detection System")
st.markdown("Listening to **Kafka Event Stream** | Enriching with **Redis Feature Store**")

# Metrics Row
col1, col2, col3, col4 = st.columns(4)
metric_safe = col1.empty()
metric_fraud = col2.empty()
metric_money = col3.empty()
status_indicator = col4.empty()

chart_placeholder = st.empty()
table_placeholder = st.empty()

# State
if 'rows' not in st.session_state:
    st.session_state.rows = []
if 'stats' not in st.session_state:
    st.session_state.stats = {'safe': 0, 'fraud': 0, 'money_saved': 0}

# --- PROCESSING LOOP ---
def process_stream():
    msg = consumer.poll(0.1)
    
    if msg is None:
        return
    if msg.error():
        st.error(f"Kafka Error: {msg.error()}")
        return

    data = json.loads(msg.value().decode('utf-8'))
    
    # 1. Feature Engineering (Redis)
    user_id = str(data.get('cardholder_age', 'unknown')) # Safety check
    key = f"user_{user_id}_count"
    velocity = redis.get(key)
    velocity = int(velocity) if velocity else 0
    redis.incr(key)
    redis.expire(key, 600)
    
    # 2. Predict
    row = pd.DataFrame([data])
    row = pd.get_dummies(row, columns=['merchant_category'])
    for col in model_columns:
        if col not in row.columns:
            row[col] = 0
    row = row[model_columns]
    
    pred = model.predict(row)[0]
    prob = model.predict_proba(row)[0][1]
    
    # 3. Update State
    is_fraud = pred == 1
    timestamp = time.strftime('%H:%M:%S')
    
    new_row = {
        'Time': timestamp,
        'ID': data.get('transaction_id', 'N/A'),
        'Amount': f"${data.get('amount', 0)}",
        'Category': data.get('merchant_category', 'N/A'),
        'Velocity (10m)': velocity,
        'Risk Score': f"{prob:.2f}",
        'Status': "🚨 FRAUD" if is_fraud else "✅ Safe"
    }
    
    st.session_state.rows.insert(0, new_row)
    st.session_state.rows = st.session_state.rows[:10]
    
    if is_fraud:
        st.session_state.stats['fraud'] += 1
        st.session_state.stats['money_saved'] += float(data.get('amount', 0))
    else:
        st.session_state.stats['safe'] += 1

    # --- REFRESH UI ---
    metric_safe.metric("Safe Transactions", st.session_state.stats['safe'])
    metric_fraud.metric("Fraud Detected", st.session_state.stats['fraud'], delta_color="inverse")
    metric_money.metric("Money Saved", f"${st.session_state.stats['money_saved']:.2f}")
    
    if is_fraud:
        status_indicator.error(f"🚨 FRAUD DETECTED! ID: {data.get('transaction_id')}")
    else:
        status_indicator.success("System Active - Monitoring...")

    df_display = pd.DataFrame(st.session_state.rows)
    table_placeholder.dataframe(df_display, use_container_width=True)

    if len(st.session_state.rows) > 0:
        chart_data = pd.DataFrame(st.session_state.rows)
        chart_data['Risk Score'] = chart_data['Risk Score'].astype(float)
        
        c = alt.Chart(chart_data).mark_line(point=True).encode(
            x='Time',
            y='Risk Score',
            color=alt.condition(alt.datum['Risk Score'] > 0.5, alt.value('red'), alt.value('green'))
        ).properties(height=300)
        chart_placeholder.altair_chart(c, use_container_width=True)

if st.button('Start/Stop Monitoring'):
    while True:
        process_stream()
        time.sleep(0.5)