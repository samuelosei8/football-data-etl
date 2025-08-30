# ------------------------------
# Football Data ETL Pipeline
# ------------------------------

# Install required packages if needed:
# pip install pandas matplotlib seaborn statsbombpy openpyxl sqlalchemy

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsbombpy import sb
from sqlalchemy import create_engine
import os

# Ensure outputs folder exists
if not os.path.exists("outputs"):
    os.makedirs("outputs")

# ------------------------------
# Step 1: EXTRACT
# ------------------------------
# Ligue 1, France 2020/21 season
matches = sb.matches(competition_id=11, season_id=42)

all_shots = []
for mid in matches.match_id:
    e = sb.events(match_id=mid, split=True)["shots"]
    if not e.empty:
        all_shots.append(e)

shots = pd.concat(all_shots)
shots = shots[['player','team','minute','shot_outcome','shot_statsbomb_xg']]
shots['player'] = shots['player'].fillna("Unknown")

# ------------------------------
# Step 2: TRANSFORM
# ------------------------------
player_stats = shots.groupby('player').agg(
    shots=('minute','count'),
    xG=('shot_statsbomb_xg','sum'),
    goals=('shot_outcome', lambda x: (x=="Goal").sum())
).reset_index()

player_stats['xG_diff'] = player_stats['goals'] - player_stats['xG']

# ------------------------------
# Step 3: LOAD
# ------------------------------
engine = create_engine("sqlite:///outputs/ligue1_data.db")
player_stats.to_sql("player_stats", engine, if_exists="replace", index=False)

player_stats.to_excel("outputs/ligue1_player_stats.xlsx", index=False)

# ------------------------------
# Step 4: OUTPUT
# ------------------------------
query = "SELECT player, goals, xG, xG_diff FROM player_stats ORDER BY xG DESC LIMIT 10;"
top10 = pd.read_sql(query, engine)
print("Top 10 players by xG:\n", top10)

plt.figure(figsize=(10,6))
sns.scatterplot(data=top10, x="xG", y="goals", hue="xG_diff", palette="coolwarm", s=100)
plt.plot([0, top10[['xG','goals']].max().max()], [0, top10[['xG','goals']].max().max()], linestyle="--", color="grey")
plt.title("Top 10 Ligue 1 Players: Goals vs Expected Goals (xG)")
plt.xlabel("Expected Goals (xG)")
plt.ylabel("Actual Goals")
plt.savefig("outputs/ligue1_goals_vs_xg.png")
plt.show()
