import pandas as pd, numpy as np, json
from datetime import date, timedelta

subs = pd.read_csv('data/raw/subscribers/subscribers.csv')
news = pd.read_parquet('data/raw/news/events.parquet')
games = pd.read_parquet('data/raw/games/events.parquet')
cooking = pd.read_parquet('data/raw/cooking/events.parquet')
athletic = pd.read_parquet('data/raw/athletic/events.parquet')

plan_dist = subs['plan_type'].value_counts().to_dict()
churn_by_plan = subs.groupby('plan_type')['is_churned'].agg(['sum','count']).reset_index()
churn_by_plan['churn_rate'] = (churn_by_plan['sum']/churn_by_plan['count']*100).round(1)
churn_dict = churn_by_plan.set_index('plan_type')[['sum','churn_rate']].to_dict('index')
acq = subs['acquisition_channel'].value_counts().to_dict()
subs['start'] = pd.to_datetime(subs['subscription_start_date'])
subs['tenure'] = (pd.Timestamp.now() - subs['start']).dt.days.clip(0)

def get_tier(p):
    if p == 'bundle_all': return 'Full Bundle'
    if p == 'bundle_3': return 'Partial Bundle'
    return 'Single Product'
subs['tier'] = subs['plan_type'].apply(get_tier)
tier_dist = subs['tier'].value_counts().to_dict()

news['date'] = pd.to_datetime(news['event_timestamp']).dt.date
games['date'] = pd.to_datetime(games['event_timestamp']).dt.date
cooking['date'] = pd.to_datetime(cooking['event_timestamp']).dt.date
athletic['date'] = pd.to_datetime(athletic['event_timestamp']).dt.date

cutoff = max(news['date'].max(), games['date'].max()) - timedelta(days=7)
weekly_news = news[news['date'] >= cutoff]['subscriber_id'].nunique()
weekly_games = games[games['date'] >= cutoff]['subscriber_id'].nunique()
weekly_cooking = cooking[cooking['date'] >= cutoff]['subscriber_id'].nunique()
weekly_athletic = athletic[athletic['date'] >= cutoff]['subscriber_id'].nunique()

games_breakdown = games[games['event_type']=='game_complete']['game_name'].value_counts().to_dict()

news['date_str'] = news['date'].astype(str)
last30 = news[news['date'] >= (news['date'].max() - timedelta(days=30))]
daily_news = last30.groupby('date_str')['subscriber_id'].nunique().sort_index().to_dict()

news_users = set(news['subscriber_id'].dropna())
games_users = set(games['subscriber_id'].dropna())
cooking_users = set(cooking['subscriber_id'].dropna())
athletic_users = set(athletic['subscriber_id'].dropna())
multi_product = {
    '1 Product': len((news_users | games_users | cooking_users | athletic_users) - ((news_users & games_users) | (news_users & cooking_users) | (news_users & athletic_users) | (games_users & cooking_users) | (games_users & athletic_users) | (cooking_users & athletic_users))),
    '2 Products': len((news_users & games_users) | (news_users & cooking_users) | (games_users & cooking_users)) - len(news_users & games_users & cooking_users),
    '3 Products': len(news_users & games_users & cooking_users) - len(news_users & games_users & cooking_users & athletic_users),
    '4 Products': len(news_users & games_users & cooking_users & athletic_users),
}

# Churn risk buckets (simplified)
def churn_risk(row):
    if row['is_churned']: return 'Churned'
    if row['tenure'] < 30: return 'New (<30d)'
    if row['plan_type'] in ('bundle_all','bundle_3'): return 'Low Risk'
    return 'Medium Risk'
subs['risk'] = subs.apply(churn_risk, axis=1)
risk_dist = subs['risk'].value_counts().to_dict()

# Section engagement
section_agg = news.groupby('section')['subscriber_id'].nunique().sort_values(ascending=False).to_dict()

data = {
    'plan_dist': plan_dist,
    'churn_dict': {k: {'churned': int(v['sum']), 'rate': float(v['churn_rate'])} for k,v in churn_dict.items()},
    'acq': acq,
    'tier_dist': tier_dist,
    'weekly_active': {'News': int(weekly_news), 'Games': int(weekly_games), 'Cooking': int(weekly_cooking), 'Athletic': int(weekly_athletic)},
    'games_breakdown': {k: int(v) for k,v in games_breakdown.items()},
    'daily_news': daily_news,
    'multi_product': {k: int(v) for k,v in multi_product.items()},
    'risk_dist': {k: int(v) for k,v in risk_dist.items()},
    'section_agg': {k: int(v) for k,v in section_agg.items()},
    'total_subs': int(len(subs)),
    'churned': int(subs['is_churned'].sum()),
    'total_events': int(len(news) + len(games) + len(cooking) + len(athletic)),
    'avg_tenure': round(float(subs[~subs['is_churned']]['tenure'].mean()), 1),
    'bundle_pct': round(float((subs['plan_type'].isin(['bundle_all','bundle_3'])).mean()*100), 1),
    'mrr_estimate': round(float(subs[~subs['is_churned']].apply(lambda r: 20 if r['plan_type']=='bundle_all' else (14 if r['plan_type']=='bundle_3' else 5), axis=1).sum()), 0),
}

import os
os.makedirs('data', exist_ok=True)
with open('data/dashboard_data.json','w') as f:
    json.dump(data, f, indent=2)
print('Done:', json.dumps({k:v for k,v in data.items() if k not in ['daily_news','section_agg','games_breakdown']}, indent=2))
