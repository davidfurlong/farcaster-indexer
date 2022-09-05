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


-- Casts per hour over the last 7 days with day label
SELECT
  date_trunc('hour', (to_timestamp(casts.published_at / 1000))) AS hour,
  COUNT(*) AS num_casts,
  date_part('dow', (to_timestamp(casts.published_at / 1000))) AS dow
FROM casts
WHERE (to_timestamp(casts.published_at / 1000) > (now() - interval '7 days'))
GROUP BY hour, dow
ORDER BY dow, hour ASC;


-- List of unique monthly casters
SELECT DISTINCT
  address
FROM
  casts
WHERE
  to_timestamp((casts.published_at / 1000))::date > (now() - '30 days'::interval);


-- Number of unique casters per week over the last year
SELECT
  date_trunc('week', (to_timestamp(casts.published_at / 1000))) AS week,
  COUNT(DISTINCT address) AS count
FROM casts
WHERE (to_timestamp(casts.published_at / 1000) > (now() - interval '1 year'))
GROUP BY week
ORDER BY week DESC;


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


-- Trending profiles over the last 7 days
WITH casts_per_profile AS (
  SELECT
    address,
    COUNT(*)
  FROM
    casts
  WHERE (to_timestamp(published_at / 1000) > (now() - interval '7 days'))
GROUP BY
  address
),
engagement_per_profile AS (
  SELECT
    address,
    COALESCE(SUM(reactions),
      0) AS reactions,
    COALESCE(SUM(recasts),
      0) AS recasts,
    COALESCE(SUM(watches),
      0) AS watches,
    COALESCE(SUM(num_reply_children),
      0) AS replies
  FROM
    casts
  WHERE (to_timestamp(published_at / 1000) > (now() - interval '7 days'))
GROUP BY
  address
),
followers_per_profile AS (
  SELECT
    address,
    avatar,
    display_name,
    username,
    followers
  FROM
    profiles
  WHERE (to_timestamp(registered_at / 1000) < (now() - interval '14 days'))
)
SELECT
  username,
  avatar,
  display_name,
  casts_per_profile.count AS casts,
  followers,
  reactions,
  recasts,
  watches,
  replies,
  ((recasts * 3) + (watches * 2) + replies + reactions) AS score
FROM
  followers_per_profile
  JOIN casts_per_profile ON followers_per_profile.address = casts_per_profile.address
  JOIN engagement_per_profile ON followers_per_profile.address = engagement_per_profile.address
WHERE
  casts_per_profile.count > 1
ORDER BY
  score DESC
LIMIT 50;
