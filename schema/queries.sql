-- Unique profiles publishing casts by day
SELECT
  to_timestamp(published_at / 1000)::date AS date,
  count(DISTINCT address) AS count
FROM
  casts
GROUP BY
  (to_timestamp((published_at / 1000))::date)
ORDER BY
  (to_timestamp((published_at / 1000))::date)
  DESC;


-- Total market cap of Farcaster connected addresses
SELECT SUM(wallet_balance) FROM profiles;


-- Casts per hour over the past week
SELECT
  date_trunc('hour', (to_timestamp(casts.published_at / 1000))) AS hour,
  COUNT(*) AS num_casts
FROM casts
GROUP BY hour
ORDER by hour DESC
LIMIT 24 * 7;


-- Unique monthly casters
SELECT DISTINCT
  address
FROM
  casts
WHERE
  to_timestamp((casts.published_at / 1000))::date > (now() - '30 days'::interval);


-- Farcaster profiles with verified NFT avatar
WITH verified_avatar AS (
  SELECT COUNT(DISTINCT address)
  FROM casts
  WHERE is_verified_avatar = TRUE
),
total_casters AS (
  SELECT COUNT(DISTINCT address)
  FROM casts
)

SELECT * FROM verified_avatar
UNION
SELECT * FROM total_casters;
