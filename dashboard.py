"""
dashboard.py — Smart City Pipeline Dashboard
Kết nối Athena → hiển thị 4 section: tốc độ, hãng xe, thời tiết, sự cố

Cài dependencies:
    pip install streamlit pandas boto3 pyathena plotly

Chạy:
    streamlit run dashboard.py
"""

import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pyathena import connect
from dotenv import load_dotenv

# ── Load env ──────────────────────────────────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), 'jobs', '.env'))

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "ap-southeast-1")
BUCKET         = os.getenv("AWS_BUCKET_NAME")
DB             = "smartcity_db"
S3_OUTPUT      = f"s3://{BUCKET}/athena-results/"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Smart City Pipeline",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600&display=swap');

  html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #0a0f1e;
    color: #e2e8f0;
  }
  h1, h2, h3 { font-family: 'Space Mono', monospace; }

  .metric-card {
    background: linear-gradient(135deg, #1a2035 0%, #0f172a 100%);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 20px 24px;
    text-align: center;
  }
  .metric-label {
    font-size: 11px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #64748b;
    margin-bottom: 6px;
  }
  .metric-value {
    font-family: 'Space Mono', monospace;
    font-size: 32px;
    font-weight: 700;
    color: #38bdf8;
  }
  .metric-sub {
    font-size: 12px;
    color: #475569;
    margin-top: 4px;
  }
  .section-header {
    font-family: 'Space Mono', monospace;
    font-size: 13px;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: #38bdf8;
    border-left: 3px solid #38bdf8;
    padding-left: 12px;
    margin: 32px 0 16px 0;
  }
  .badge-active {
    background: #dc2626;
    color: white;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
  }
  .badge-resolved {
    background: #16a34a;
    color: white;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 11px;
  }
  div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
  .stSpinner > div { border-top-color: #38bdf8 !important; }
</style>
""", unsafe_allow_html=True)

PLOTLY_THEME = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(15,23,42,0.6)",
    font_color="#94a3b8",
    font_family="DM Sans",
    colorway=["#38bdf8", "#818cf8", "#34d399", "#fb923c", "#f472b6"],
    xaxis=dict(gridcolor="#1e293b", linecolor="#334155"),
    yaxis=dict(gridcolor="#1e293b", linecolor="#334155"),
)

# ── Athena connection ─────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return connect(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
        s3_staging_dir=S3_OUTPUT,
        schema_name=DB,
    )

@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, get_conn())

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_all():
    vehicle = query("""
        SELECT timestamp, speed, speed_kmh, make, model, fuelType, is_ev, hour, date
        FROM vehicle_data
        ORDER BY timestamp DESC
        LIMIT 500
    """)
    weather = query("""
        SELECT timestamp, temperature, weatherCondition,
               precipitation, windSpeed, humidity, airQualityIndex,
               heat_index, aqi_category, date
        FROM weather_data
        ORDER BY timestamp DESC
        LIMIT 500
    """)
    emergency = query("""
        SELECT timestamp, type, status, location, description,
               is_active, is_real_incident, date
        FROM emergency_data
        ORDER BY timestamp DESC
        LIMIT 500
    """)
    return vehicle, weather, emergency

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="padding: 8px 0 24px 0;">
  <div style="font-family:'Space Mono',monospace; font-size:11px; letter-spacing:4px;
              color:#38bdf8; text-transform:uppercase; margin-bottom:6px;">
    Real-Time Data Pipeline
  </div>
  <h1 style="margin:0; font-size:36px; color:#f1f5f9;">
    🏙️ Smart City Dashboard
  </h1>
  <div style="color:#475569; font-size:14px; margin-top:6px;">
    HCM → Vũng Tàu Expressway &nbsp;·&nbsp; Apache Kafka + Spark + AWS Athena
  </div>
</div>
""", unsafe_allow_html=True)

# ── Load ──────────────────────────────────────────────────────────────────────
with st.spinner("Connecting to Athena..."):
    try:
        vehicle_df, weather_df, emergency_df = load_all()
    except Exception as e:
        st.error(f"❌ Athena connection failed: {e}")
        st.stop()

# Parse timestamps
for df in [vehicle_df, weather_df, emergency_df]:
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])

# ── KPI Cards ─────────────────────────────────────────────────────────────────
active_incidents = int(emergency_df[emergency_df['status'] == 'Active'].shape[0])
real_incidents   = int(emergency_df[emergency_df['is_real_incident'] == True].shape[0]) if 'is_real_incident' in emergency_df.columns else 0
avg_speed        = round(vehicle_df['speed'].mean(), 1) if not vehicle_df.empty else 0
avg_temp         = round(weather_df['temperature'].mean(), 1) if not weather_df.empty else 0
ev_pct           = round(vehicle_df['is_ev'].mean() * 100, 1) if 'is_ev' in vehicle_df.columns and not vehicle_df.empty else 0

c1, c2, c3, c4, c5 = st.columns(5)
for col, label, value, sub in [
    (c1, "Avg Speed",       f"{avg_speed}",      "km/h"),
    (c2, "Active Incidents",f"{active_incidents}","real-time"),
    (c3, "Avg Temperature", f"{avg_temp}°",       "Celsius"),
    (c4, "EV Rate",         f"{ev_pct}%",         "electric vehicles"),
    (c5, "Total Records",   f"{len(vehicle_df)}", "vehicle events"),
]:
    col.markdown(f"""
    <div class="metric-card">
      <div class="metric-label">{label}</div>
      <div class="metric-value">{value}</div>
      <div class="metric-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Tốc độ xe theo thời gian
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header">01 — Vehicle Speed Over Time</div>', unsafe_allow_html=True)

col_l, col_r = st.columns([2, 1])

with col_l:
    if not vehicle_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=vehicle_df['timestamp'], y=vehicle_df['speed'],
            mode='lines', name='Speed (km/h)',
            line=dict(color='#38bdf8', width=2),
            fill='tozeroy', fillcolor='rgba(56,189,248,0.08)'
        ))
        fig.add_hline(y=vehicle_df['speed'].mean(), line_dash="dash",
                      line_color="#818cf8", annotation_text="avg",
                      annotation_font_color="#818cf8")
        fig.update_layout(title="Speed Timeline", **PLOTLY_THEME,
                          height=300, margin=dict(l=0, r=0, t=36, b=0),
                          showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

with col_r:
    if 'hour' in vehicle_df.columns:
        hourly = vehicle_df.groupby('hour')['speed'].mean().reset_index()
        fig2 = px.bar(hourly, x='hour', y='speed',
                      title="Avg Speed by Hour",
                      color='speed', color_continuous_scale='Blues')
        fig2.update_layout(**PLOTLY_THEME, height=300,
                           margin=dict(l=0, r=0, t=36, b=0),
                           coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Hãng xe / Nhiên liệu
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header">02 — Vehicle & Fuel Statistics</div>', unsafe_allow_html=True)

col_a, col_b, col_c = st.columns(3)

with col_a:
    if 'make' in vehicle_df.columns:
        make_counts = vehicle_df['make'].value_counts().reset_index()
        make_counts.columns = ['make', 'count']
        fig = px.bar(make_counts, x='count', y='make', orientation='h',
                     title="Vehicles by Brand",
                     color='count', color_continuous_scale='Blues')
        fig.update_layout(**PLOTLY_THEME, height=300,
                          margin=dict(l=0, r=0, t=36, b=0),
                          coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

with col_b:
    if 'fuelType' in vehicle_df.columns:
        fuel_counts = vehicle_df['fuelType'].value_counts().reset_index()
        fuel_counts.columns = ['fuelType', 'count']
        fig = px.pie(fuel_counts, values='count', names='fuelType',
                     title="Fuel Type Distribution",
                     color_discrete_sequence=['#38bdf8', '#818cf8', '#34d399'])
        fig.update_layout(**PLOTLY_THEME, height=300,
                          margin=dict(l=0, r=0, t=36, b=0))
        fig.update_traces(textfont_color='white')
        st.plotly_chart(fig, use_container_width=True)

with col_c:
    if 'make' in vehicle_df.columns and 'speed' in vehicle_df.columns:
        make_speed = vehicle_df.groupby('make')['speed'].agg(['mean', 'max']).reset_index()
        make_speed.columns = ['make', 'avg_speed', 'max_speed']
        fig = go.Figure()
        fig.add_trace(go.Bar(name='Avg Speed', x=make_speed['make'],
                             y=make_speed['avg_speed'], marker_color='#38bdf8'))
        fig.add_trace(go.Bar(name='Max Speed', x=make_speed['make'],
                             y=make_speed['max_speed'], marker_color='#818cf8'))
        fig.update_layout(title="Speed by Brand", barmode='group',
                          **PLOTLY_THEME, height=300,
                          margin=dict(l=0, r=0, t=36, b=0))
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Thời tiết dọc tuyến
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header">03 — Weather Along the Route</div>', unsafe_allow_html=True)

col_p, col_q = st.columns([2, 1])

with col_p:
    if not weather_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=weather_df['timestamp'], y=weather_df['temperature'],
            name='Temperature (°C)', line=dict(color='#fb923c', width=2),
            fill='tozeroy', fillcolor='rgba(251,146,60,0.08)'
        ))
        fig.add_trace(go.Scatter(
            x=weather_df['timestamp'], y=weather_df['humidity'],
            name='Humidity (%)', line=dict(color='#38bdf8', width=2),
            yaxis='y2'
        ))
        fig.update_layout(
            title="Temperature & Humidity Over Time",
            yaxis2=dict(overlaying='y', side='right',
                        gridcolor='rgba(0,0,0,0)', tickfont_color='#38bdf8'),
            **PLOTLY_THEME, height=300,
            margin=dict(l=0, r=40, t=36, b=0),
            legend=dict(orientation='h', y=-0.15,
                        font_color='#94a3b8', bgcolor='rgba(0,0,0,0)')
        )
        st.plotly_chart(fig, use_container_width=True)

with col_q:
    if 'weatherCondition' in weather_df.columns:
        cond_counts = weather_df['weatherCondition'].value_counts().reset_index()
        cond_counts.columns = ['condition', 'count']
        colors = {'Sunny': '#fb923c', 'Cloudy': '#94a3b8',
                  'Rain': '#38bdf8', 'Stormy': '#818cf8'}
        fig = px.pie(cond_counts, values='count', names='condition',
                     title="Weather Conditions",
                     color='condition',
                     color_discrete_map=colors)
        fig.update_layout(**PLOTLY_THEME, height=300,
                          margin=dict(l=0, r=0, t=36, b=0))
        fig.update_traces(textfont_color='white')
        st.plotly_chart(fig, use_container_width=True)

# AQI bar
if 'aqi_category' in weather_df.columns:
    aqi_counts = weather_df['aqi_category'].value_counts().reset_index()
    aqi_counts.columns = ['category', 'count']
    aqi_colors = {'Good': '#34d399', 'Moderate': '#fbbf24',
                  'Unhealthy for Sensitive': '#fb923c',
                  'Unhealthy': '#f87171', 'Very Unhealthy': '#818cf8'}
    fig = px.bar(aqi_counts, x='category', y='count',
                 title="Air Quality Index Distribution",
                 color='category', color_discrete_map=aqi_colors)
    fig.update_layout(**PLOTLY_THEME, height=220,
                      margin=dict(l=0, r=0, t=36, b=0), showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Sự cố khẩn cấp
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-header">04 — Emergency Incidents</div>', unsafe_allow_html=True)

col_x, col_y = st.columns([1, 2])

with col_x:
    if 'type' in emergency_df.columns:
        type_counts = emergency_df[emergency_df['type'] != 'None']['type'].value_counts().reset_index()
        type_counts.columns = ['type', 'count']
        fig = px.bar(type_counts, x='type', y='count',
                     title="Incidents by Type",
                     color='type',
                     color_discrete_sequence=['#f87171', '#fb923c', '#fbbf24', '#818cf8'])
        fig.update_layout(**PLOTLY_THEME, height=300,
                          margin=dict(l=0, r=0, t=36, b=0), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

with col_y:
    # Active incidents table
    active = emergency_df[
        (emergency_df['status'] == 'Active') &
        (emergency_df['type'] != 'None')
    ][['timestamp', 'type', 'status', 'description']].head(10)

    if active.empty:
        st.info("✅ No active incidents on the route.")
    else:
        st.markdown(f"**🚨 Active Incidents ({len(active)})**")

        def style_row(row):
            if row['status'] == 'Active':
                return ['background-color: rgba(220,38,38,0.1)'] * len(row)
            return [''] * len(row)

        styled = active.style.apply(style_row, axis=1)
        st.dataframe(styled, use_container_width=True, height=280,
                     hide_index=True)

# Timeline of incidents
if not emergency_df.empty and 'type' in emergency_df.columns:
    timeline = emergency_df[emergency_df['type'] != 'None'].copy()
    timeline = timeline.groupby([pd.Grouper(key='timestamp', freq='1h'), 'type']).size().reset_index(name='count')
    if not timeline.empty:
        fig = px.bar(timeline, x='timestamp', y='count', color='type',
                     title="Incident Timeline (hourly)",
                     color_discrete_sequence=['#f87171', '#fb923c', '#fbbf24', '#818cf8'])
        fig.update_layout(**PLOTLY_THEME, height=240,
                          margin=dict(l=0, r=0, t=36, b=0),
                          legend=dict(orientation='h', y=-0.2,
                                      bgcolor='rgba(0,0,0,0)',
                                      font_color='#94a3b8'))
        st.plotly_chart(fig, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-top:40px; padding-top:20px; border-top:1px solid #1e293b;
            text-align:center; color:#334155; font-size:12px;">
  Smart City Data Pipeline &nbsp;·&nbsp; HCM → Vũng Tàu &nbsp;·&nbsp;
  Kafka · Spark · S3 · Athena · Streamlit
</div>
""", unsafe_allow_html=True)