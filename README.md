# Engineering-a-Trending-Now-Algorithm-Using-Pyspark

### About
SoundWave's current "Top Podcasts" chart is based on simple all-time listen counts, which creates a feedback loop where already popular shows remain at the top, making it nearly impossible for new and emerging creators to get noticed. This stale ranking system leads to poor user engagement and creator dissatisfaction.

### Challenge
The Org needs to replace its static chart with a dynamic "Trending Now" algorithm. This requires an engine that can process millions of daily listening events to not only measure popularity but also capture momentum and velocity. The core challenge is to design and implement a scalable PySpark job that can perform complex time-series analysis, calculate rolling metrics, and apply a weighted scoring formula to produce a fair and engaging ranking.

### Solution
I as a Senior Data Engineer, on the Content Discovery team, designed and built the core PySpark logic that powers the new "Trending Now" feature. The solution is very efficient, robust, and capable of transforming raw listening data into a ranked list that accurately reflects what's currently capturing listener attention.

### Details
This project builds a Databricks-PySpark based logic to identify trending podcasts from raw listening data. Three CSV datasets are ingested and joined to create an enriched dataset linking each listening event to its podcast metadata and category. Timestamps are converted and normalized to a daily level for time-series analysis.

Daily listen counts are aggregated per podcast and enhanced using window functions. A Popularity Score is calculated as a 7-day rolling average of listens, while a Momentum Score captures week-over-week growth, with safeguards to handle zero-value edge cases.

These features are combined into a final Trending Score using a weighted formula that favors recent momentum over raw popularity. Podcasts are then ranked to produce an overall “Trending Now” chart, as well as category-specific rankings to highlight trends within each genre.


 ADLS Landing zone             |  Parameterization logic
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/d4d68771-28a1-4e13-a41a-d4618cfab321" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/bad465af-fa4e-4689-8974-30bc77b187fa" />


 Ingestion logic             |  Adding features
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/62c0382b-b5aa-4350-ad97-45b0b6d0ce2e" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/e0b5e1cb-0a8b-4805-9dc6-1b3a75320f3b" />


 Trending score logic             |  Output for extraction / external use
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/d78e8343-f7d1-4f77-bbc6-6d823641e620" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/71221a46-6ca4-4313-a395-928a334a62b5" />




