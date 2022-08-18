-- Profile Table Definition
CREATE TABLE profiles (
  "index" int4 NOT NULL PRIMARY KEY,
  "merkle_root" text NOT NULL,
  "signature" text,
  "username" text,
  "display_name" text,
  "followers" int4,
  "address_activity" text,
  "avatar" text,
  "proof" text,
  "timestamp" int8,
  "version" int2,
  "address" text,
  "connected_address" text,
  "wallet_balance" int4,
);

-- Casts Table Definition
CREATE TABLE casts (
  "published_at" int8 NOT NULL,
  "sequence" int4 NOT NULL,
  "username" text NOT NULL,
  "address" text NOT NULL,
  "text" text NOT NULL,
  "reply_parent_merkle_root" text,
  "prev_merkle_root" text,
  "merkle_root" text NOT NULL PRIMARY KEY,
  "signature" text NOT NULL,
  "display_name" text,
  "avatar" text,
  "is_verified_avatar" bool,
  "num_reply_children" int2,
  "reactions" int2,
  "recasts" int2,
  "watches" int2,
  "reply_parent_username" text,
);
