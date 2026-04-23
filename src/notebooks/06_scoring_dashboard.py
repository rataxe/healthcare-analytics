# Fabric Notebook — Batch Scoring Dashboard: Visualisera ML-prediktioner
# Kör efter 05_batch_scoring.py
# Läser resultat från Gold Lakehouse och skapar interaktiva visualiseringar

# ── PARAMETERCELL ──────────────────────────────────────────────────────────────
GOLD_LAKEHOUSE       = "gold_lakehouse"
WORKSPACE_ID         = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
GOLD_LAKEHOUSE_ID    = "2960eef0-5de6-4117-80b1-6ee783cdaeec"
SAMPLE_SIZE          = 3000  # Antal rader för scatter plots (prestanda)

# ── CELL 1: Importer & setup ───────────────────────────────────────────────────
import logging
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("BatchDashboard")
spark = SparkSession.builder.getOrCreate()

# Microsoft Style Palette
MS_BLUE     = "#0078D4"
MS_TEAL     = "#008272"
MS_GREEN    = "#107C10"
MS_PURPLE   = "#5C2D91"
MS_ORANGE   = "#FF8C00"
MS_RED      = "#D13438"
MS_GRAY     = "#F3F2F1"
MS_DARK     = "#323130"

RISK_COLORS = {"Låg": MS_GREEN, "Medel": MS_ORANGE, "Hög": MS_RED, "Mycket hög": MS_PURPLE}
DEPT_PALETTE = px.colors.qualitative.Set2
TEMPLATE = "plotly_white"

log.info("✅ Dashboard initialiserad")

# ── CELL 2: Ladda data från Gold Lakehouse ─────────────────────────────────────
df_spark = spark.table(f"{GOLD_LAKEHOUSE}.batch_scoring_results")
df = df_spark.toPandas()

df_hr_spark = spark.table(f"{GOLD_LAKEHOUSE}.high_risk_patients")
df_hr = df_hr_spark.toPandas()

log.info("📊 Laddade %d prediktioner och %d högriskpatienter", len(df), len(df_hr))

# ── CELL 3: Dataprep & KPI-beräkning ───────────────────────────────────────────
df["admission_date"] = pd.to_datetime(df["admission_date"], errors="coerce")
df["discharge_date"] = pd.to_datetime(df["discharge_date"], errors="coerce")
df["los_error"] = df["predicted_los_days"] - df["los_days"]
df["los_abs_error"] = df["los_error"].abs()
df["admission_month"] = df["admission_date"].dt.to_period("M").astype(str)

risk_order = ["Låg", "Medel", "Hög", "Mycket hög"]
df["risk_category"] = pd.Categorical(df["risk_category"], categories=risk_order, ordered=True)

n_total = len(df)
n_high  = len(df_hr)
mean_los = df["predicted_los_days"].mean()
mean_readm = df["readmission_probability"].mean()
mae = df["los_abs_error"].mean()

print(f"📈 KPIs: {n_total:,} encounters | {n_high:,} högrisk | Medel LOS: {mean_los:.1f}d | Medel readm: {mean_readm:.1%} | MAE: {mae:.1f}d")

# ── CELL 4: KPI Scorecards ─────────────────────────────────────────────────────
fig_kpi = go.Figure()

kpis = [
    ("Totalt scorade", f"{n_total:,}", MS_BLUE),
    ("Högriskpatienter", f"{n_high:,} ({n_high/n_total:.0%})", MS_RED),
    ("Medel LOS-pred.", f"{mean_los:.1f} dagar", MS_TEAL),
    ("Medel readm.risk", f"{mean_readm:.1%}", MS_ORANGE),
    ("LOS MAE", f"{mae:.1f} dagar", MS_PURPLE),
]

for i, (title, val, color) in enumerate(kpis):
    fig_kpi.add_annotation(
        x=(i/5 + (i+1)/5) / 2, y=0.65,
        text=f"<b>{title}</b>",
        font=dict(size=14, color=MS_DARK, family="Segoe UI"),
        showarrow=False, xref="paper", yref="paper",
    )
    fig_kpi.add_annotation(
        x=(i/5 + (i+1)/5) / 2, y=0.35,
        text=f"<b>{val}</b>",
        font=dict(size=28, color=color, family="Segoe UI"),
        showarrow=False, xref="paper", yref="paper",
    )

fig_kpi.update_layout(
    height=130, margin=dict(t=10, b=10, l=20, r=20),
    paper_bgcolor=MS_GRAY, plot_bgcolor=MS_GRAY,
    xaxis=dict(visible=False), yaxis=dict(visible=False),
)
fig_kpi.show()
log.info("✅ KPI scorecards")

# ── CELL 5: Riskfördelning — Donut + Bar ───────────────────────────────────────
risk_counts = df["risk_category"].value_counts().reindex(risk_order)
colors_list = [RISK_COLORS[r] for r in risk_order]

fig_risk = make_subplots(
    rows=1, cols=2,
    specs=[[{"type": "pie"}, {"type": "bar"}]],
    subplot_titles=("Fördelning per riskkategori", "Antal per riskkategori"),
)

fig_risk.add_trace(go.Pie(
    labels=risk_order, values=risk_counts.values,
    hole=0.55, marker_colors=colors_list,
    textinfo="percent+label", textfont=dict(size=12, family="Segoe UI"),
    hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
), row=1, col=1)

fig_risk.add_trace(go.Bar(
    x=risk_order, y=risk_counts.values,
    marker_color=colors_list,
    text=risk_counts.values, textposition="outside",
    textfont=dict(family="Segoe UI", size=13),
    hovertemplate="%{x}: %{y:,}<extra></extra>",
), row=1, col=2)

fig_risk.update_layout(
    height=400, template=TEMPLATE, showlegend=False,
    font=dict(family="Segoe UI"), title_font_size=16,
    margin=dict(t=50, b=40),
)
fig_risk.show()
log.info("✅ Riskfördelning")

# ── CELL 6: Readmission-sannolikhet — Histogram + Box per avdelning ────────────
fig_readm = make_subplots(
    rows=1, cols=2,
    subplot_titles=(
        "Återinskrivningssannolikhet — fördelning",
        "Återinskrivningssannolikhet per avdelning",
    ),
)

fig_readm.add_trace(go.Histogram(
    x=df["readmission_probability"], nbinsx=50,
    marker_color=MS_BLUE, opacity=0.8,
    hovertemplate="Prob: %{x:.2f}<br>Antal: %{y}<extra></extra>",
    name="",
), row=1, col=1)

fig_readm.add_vline(
    x=0.40, line_dash="dash", line_color=MS_RED, line_width=2,
    annotation_text="Högrisk-gräns (40%)",
    annotation_font=dict(color=MS_RED, size=11, family="Segoe UI"),
    row=1, col=1,
)

depts = df["department"].value_counts().index.tolist()
for i, dept in enumerate(depts):
    sub = df[df["department"] == dept]
    fig_readm.add_trace(go.Box(
        y=sub["readmission_probability"], name=dept,
        marker_color=DEPT_PALETTE[i % len(DEPT_PALETTE)],
        boxmean=True,
    ), row=1, col=2)

fig_readm.update_layout(
    height=420, template=TEMPLATE, showlegend=False,
    font=dict(family="Segoe UI"),
    margin=dict(t=50, b=40),
)
fig_readm.update_xaxes(title_text="Sannolikhet", row=1, col=1)
fig_readm.update_yaxes(title_text="Antal encounters", row=1, col=1)
fig_readm.update_yaxes(title_text="Sannolikhet", row=1, col=2)
fig_readm.show()
log.info("✅ Readmission-distribution")

# ── CELL 7: LOS — Predicted vs Actual + Residualer ─────────────────────────────
sample = df.sample(n=min(SAMPLE_SIZE, len(df)), random_state=42)

fig_los = make_subplots(
    rows=1, cols=2,
    subplot_titles=(
        "Predicted vs Actual LOS (dagar)",
        "Residualer (prediktionsfel)",
    ),
)

fig_los.add_trace(go.Scattergl(
    x=sample["los_days"], y=sample["predicted_los_days"],
    mode="markers",
    marker=dict(
        size=4, opacity=0.4,
        color=sample["readmission_probability"],
        colorscale=[[0, MS_GREEN], [0.4, MS_ORANGE], [1, MS_RED]],
        colorbar=dict(title="Readm.prob", len=0.6, x=0.44),
    ),
    hovertemplate="Actual: %{x:.0f}d<br>Pred: %{y:.1f}d<extra></extra>",
    name="",
), row=1, col=1)

max_los = max(sample["los_days"].max(), sample["predicted_los_days"].max())
fig_los.add_trace(go.Scattergl(
    x=[0, max_los], y=[0, max_los],
    mode="lines", line=dict(color=MS_DARK, dash="dash", width=1),
    name="Perfekt", showlegend=False,
), row=1, col=1)

fig_los.add_trace(go.Histogram(
    x=sample["los_error"], nbinsx=60,
    marker_color=MS_TEAL, opacity=0.8,
    hovertemplate="Fel: %{x:.1f}d<br>Antal: %{y}<extra></extra>",
    name="",
), row=1, col=2)

fig_los.add_vline(x=0, line_color=MS_DARK, line_dash="dash", line_width=1, row=1, col=2)

fig_los.update_layout(
    height=420, template=TEMPLATE, showlegend=False,
    font=dict(family="Segoe UI"),
    margin=dict(t=50, b=40),
)
fig_los.update_xaxes(title_text="Actual LOS (dagar)", row=1, col=1)
fig_los.update_yaxes(title_text="Predicted LOS (dagar)", row=1, col=1)
fig_los.update_xaxes(title_text="Prediktionsfel (dagar)", row=1, col=2)
fig_los.update_yaxes(title_text="Antal", row=1, col=2)
fig_los.show()
log.info("✅ LOS predicted vs actual")

# ── CELL 8: Avdelningsanalys — Risk + LOS ──────────────────────────────────────
dept_stats = (
    df.groupby("department")
    .agg(
        n_encounters=("encounter_id", "count"),
        avg_readm_prob=("readmission_probability", "mean"),
        avg_pred_los=("predicted_los_days", "mean"),
        avg_actual_los=("los_days", "mean"),
        pct_high_risk=("readmission_predicted", "mean"),
    )
    .sort_values("avg_readm_prob", ascending=True)
    .reset_index()
)

fig_dept = make_subplots(
    rows=1, cols=2,
    subplot_titles=(
        "Medel återinskrivningsrisk per avdelning",
        "Vårdtid: Actual vs Predicted per avdelning",
    ),
)

fig_dept.add_trace(go.Bar(
    y=dept_stats["department"], x=dept_stats["avg_readm_prob"],
    orientation="h", marker_color=MS_BLUE,
    text=[f"{v:.1%}" for v in dept_stats["avg_readm_prob"]],
    textposition="outside",
    textfont=dict(family="Segoe UI", size=11),
    hovertemplate="%{y}: %{x:.1%}<extra></extra>",
    name="",
), row=1, col=1)

fig_dept.add_trace(go.Bar(
    y=dept_stats["department"], x=dept_stats["avg_actual_los"],
    orientation="h", marker_color=MS_TEAL, name="Actual LOS",
    hovertemplate="%{y}: %{x:.1f} dagar<extra>Actual</extra>",
), row=1, col=2)

fig_dept.add_trace(go.Bar(
    y=dept_stats["department"], x=dept_stats["avg_pred_los"],
    orientation="h", marker_color=MS_ORANGE, name="Predicted LOS",
    hovertemplate="%{y}: %{x:.1f} dagar<extra>Predicted</extra>",
), row=1, col=2)

fig_dept.update_layout(
    height=450, template=TEMPLATE,
    font=dict(family="Segoe UI"),
    barmode="group", legend=dict(orientation="h", y=-0.08),
    margin=dict(t=50, b=60, l=100),
)
fig_dept.update_xaxes(title_text="Återinskrivningsrisk", tickformat=".0%", row=1, col=1)
fig_dept.update_xaxes(title_text="Vårdtid (dagar)", row=1, col=2)
fig_dept.show()
log.info("✅ Avdelningsanalys")

# ── CELL 9: Riskkarta — Avdelning × Riskkategori (Heatmap) ─────────────────────
cross = pd.crosstab(df["department"], df["risk_category"], normalize="index")
cross = cross.reindex(columns=risk_order)

fig_heat = go.Figure(go.Heatmap(
    z=cross.values * 100,
    x=cross.columns.tolist(),
    y=cross.index.tolist(),
    colorscale=[[0, "#E8F5E9"], [0.33, "#FFF3E0"], [0.66, "#FFEBEE"], [1, "#F3E5F5"]],
    text=[[f"{v:.1f}%" for v in row] for row in cross.values * 100],
    texttemplate="%{text}",
    textfont=dict(size=13, family="Segoe UI"),
    hovertemplate="Avd: %{y}<br>Risk: %{x}<br>Andel: %{z:.1f}%<extra></extra>",
    colorbar=dict(title="%", ticksuffix="%"),
))

fig_heat.update_layout(
    title="Andel per riskkategori per avdelning",
    height=400, template=TEMPLATE,
    font=dict(family="Segoe UI"),
    xaxis_title="Riskkategori", yaxis_title="Avdelning",
    margin=dict(t=50, b=40, l=100),
)
fig_heat.show()
log.info("✅ Riskkarta heatmap")

# ── CELL 10: Tidsmönster — Risk över tid ───────────────────────────────────────
monthly = (
    df.groupby("admission_month")
    .agg(
        n_encounters=("encounter_id", "count"),
        avg_readm=("readmission_probability", "mean"),
        pct_high_risk=("readmission_predicted", "mean"),
        avg_los=("predicted_los_days", "mean"),
    )
    .reset_index()
    .sort_values("admission_month")
)

fig_time = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    subplot_titles=(
        "Återinskrivningsrisk & högriskandel över tid",
        "Antal encounters per månad",
    ),
    row_heights=[0.6, 0.4],
    vertical_spacing=0.10,
)

fig_time.add_trace(go.Scatter(
    x=monthly["admission_month"], y=monthly["avg_readm"],
    mode="lines+markers", name="Medel readm.prob",
    line=dict(color=MS_BLUE, width=2),
    marker=dict(size=5),
    hovertemplate="%{x}<br>Medel: %{y:.1%}<extra></extra>",
), row=1, col=1)

fig_time.add_trace(go.Scatter(
    x=monthly["admission_month"], y=monthly["pct_high_risk"],
    mode="lines+markers", name="Andel högrisk",
    line=dict(color=MS_RED, width=2, dash="dot"),
    marker=dict(size=5),
    hovertemplate="%{x}<br>Högrisk: %{y:.1%}<extra></extra>",
), row=1, col=1)

fig_time.add_trace(go.Bar(
    x=monthly["admission_month"], y=monthly["n_encounters"],
    marker_color=MS_TEAL, opacity=0.7, name="Encounters",
    hovertemplate="%{x}<br>Antal: %{y:,}<extra></extra>",
), row=2, col=1)

fig_time.update_layout(
    height=500, template=TEMPLATE,
    font=dict(family="Segoe UI"),
    legend=dict(orientation="h", y=1.08),
    margin=dict(t=70, b=40),
)
fig_time.update_yaxes(title_text="Sannolikhet", tickformat=".0%", row=1, col=1)
fig_time.update_yaxes(title_text="Antal", row=2, col=1)
fig_time.show()
log.info("✅ Tidsmönster")

# ── CELL 11: Korrelationsanalys ────────────────────────────────────────────────
numeric_cols = ["readmission_probability", "predicted_los_days", "los_days"]
for c in df.columns:
    if df[c].dtype in ["float64", "float32", "int64", "int32"] and c not in numeric_cols:
        if c not in ["readmission_predicted"]:
            numeric_cols.append(c)

avail = [c for c in numeric_cols if c in df.columns]
corr = df[avail].corr()
readm_corr = corr["readmission_probability"].drop("readmission_probability").sort_values()

fig_corr = go.Figure(go.Bar(
    y=readm_corr.index,
    x=readm_corr.values,
    orientation="h",
    marker_color=[MS_RED if v > 0 else MS_BLUE for v in readm_corr.values],
    text=[f"{v:.3f}" for v in readm_corr.values],
    textposition="outside",
    textfont=dict(family="Segoe UI", size=11),
))

fig_corr.update_layout(
    title="Korrelation med återinskrivningssannolikhet",
    height=max(300, len(readm_corr) * 35 + 80),
    template=TEMPLATE,
    font=dict(family="Segoe UI"),
    xaxis_title="Pearson-korrelation",
    margin=dict(t=50, b=40, l=160),
)
fig_corr.show()
log.info("✅ Korrelationsanalys")

# ── CELL 12: Top 20 Högriskpatienter ───────────────────────────────────────────
top20 = df_hr.nlargest(20, "readmission_probability").copy()
display_cols = ["patient_id", "department", "readmission_probability",
                "predicted_los_days", "los_days", "risk_category",
                "admission_date", "discharge_date"]
display_cols = [c for c in display_cols if c in top20.columns]
top20 = top20[display_cols].reset_index(drop=True)

if "patient_id" in top20.columns:
    top20["patient_id"] = top20["patient_id"].str[:8] + "..."

row_colors = [MS_GRAY if i % 2 == 0 else "white" for i in range(len(top20))]

fig_table = go.Figure(go.Table(
    header=dict(
        values=[f"<b>{c}</b>" for c in top20.columns],
        fill_color=MS_BLUE,
        font=dict(color="white", size=12, family="Segoe UI"),
        align="left",
        height=32,
    ),
    cells=dict(
        values=[top20[c] for c in top20.columns],
        fill_color=[row_colors * len(top20.columns)],
        font=dict(size=11, family="Segoe UI"),
        align="left",
        height=28,
        format=["", "", ".3f", ".1f", ".0f", "", "", ""],
    ),
))

fig_table.update_layout(
    title="Top 20 högriskpatienter (återinskrivning)",
    height=650, template=TEMPLATE,
    font=dict(family="Segoe UI"),
    margin=dict(t=50, b=20),
)
fig_table.show()
log.info("✅ Top 20 högriskpatienter")

# ── CELL 13: Sammanfattning ────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("  📊 BATCH SCORING DASHBOARD — SAMMANFATTNING")
print("=" * 70)
print(f"\n  Modeller:")
print(f"    • LightGBM (LOS-prediktion)")
print(f"    • Random Forest (Återinskrivning)")
print(f"\n  Dataset:")
print(f"    • Scorade encounters:        {n_total:>10,}")
print(f"    • Högriskpatienter (>40%):  {n_high:>10,} ({n_high/n_total:>6.1%})")
print(f"\n  Prestanda:")
print(f"    • Medel LOS-prediktion:      {mean_los:>10.1f} dagar")
print(f"    • Medel återinskrivningsrisk:{mean_readm:>10.1%}")
print(f"    • LOS MAE:                   {mae:>10.1f} dagar")
print(f"\n  Riskfördelning:")
for cat in risk_order:
    cnt = risk_counts[cat]
    pct = cnt / n_total * 100
    print(f"    • {cat:15s}  {cnt:>7,}  ({pct:>5.1f}%)")
print("\n  ✅ Dashboard komplett — alla visualiseringar genererade")
print("=" * 70)
